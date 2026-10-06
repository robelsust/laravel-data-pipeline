<?php

declare(strict_types=1);

namespace Robel\DataPipeline;

use Closure;
use Illuminate\Support\Facades\DB;
use LogicException;
use Robel\DataPipeline\Contracts\ChunkStageInterface;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\Events\PipelineRecordFailed;
use Robel\DataPipeline\Exceptions\PipelineBusyException;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\ChunkExecutionResult;
use Robel\DataPipeline\Results\RecordErrorBag;
use Robel\DataPipeline\Results\RecordFailure;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Throwable;

/** Executes the same stage, persistence, and checkpoint contract for every delivery mode. */
final class ChunkExecutor
{
    /**
     * @param  array<int, RecordStageInterface|ChunkStageInterface>  $stages
     * @param  array<int, string>  $redactedAttributes
     */
    public function execute(
        RecordChunk $chunk,
        array $stages,
        ?SinkInterface $sink,
        ?PipelineRepositoryInterface $repository = null,
        ?string $runId = null,
        bool $allowPartialSuccess = true,
        bool $transactional = true,
        bool $allowCrossConnectionTransactions = false,
        array $redactedAttributes = [],
        ?string $leaseToken = null,
        bool $recordCheckpoint = true,
        ?Closure $afterPersistence = null,
    ): ChunkExecutionResult {
        $tracking = config('data-pipeline.connection') ?? config('database.default');
        $destination = $sink !== null && method_exists($sink, 'getConnection') ? $sink->getConnection() : null;
        $destination ??= config('database.default');
        if ($transactional && $destination !== $tracking && ! $allowCrossConnectionTransactions && $sink !== null) {
            throw new LogicException("Transactional pipeline execution cannot guarantee dual-connection atomicity across destination connection '{$destination}' and tracking connection '{$tracking}'. Call ->allowCrossConnectionTransactions() to explicitly allow separate commits.");
        }
        $work = function () use ($chunk, $stages, $sink, $repository, $runId, $allowPartialSuccess, $redactedAttributes, $leaseToken, $recordCheckpoint, $afterPersistence): ChunkExecutionResult {
            if ($leaseToken !== null && ! PipelineChunk::where('pipeline_id', $runId)->where('sequence', $chunk->sequence())->where('lease_token', $leaseToken)->lockForUpdate()->exists()) {
                throw new PipelineBusyException('Pipeline chunk lease has changed owners.');
            }
            $processed = $this->processStages($chunk, $stages, $runId);
            $strictFailure = ! $allowPartialSuccess && $processed->invalid() !== [];
            $candidates = $processed->valid();
            $sinkResult = $strictFailure
                ? SinkResult::success(0)
                : ($sink?->persist($processed) ?? SinkResult::success(count($candidates)));
            if (! $strictFailure && $sink !== null) {
                $this->attributeSinkFailures($candidates, $sinkResult);
            }
            $failures = [];
            foreach ($processed->invalid() as $record) {
                $failures[] = new RecordFailure($record->index(), $record->identifier(), $record->raw(), $record->errors());
            }
            if (! $allowPartialSuccess && ! $strictFailure && $failures !== []) {
                $bag = new RecordErrorBag;
                foreach ($failures as $failure) {
                    $bag->add($failure);
                }
                throw new PipelineValidationException('Pipeline chunk failed sink persistence under strict failIfAny policy.', $bag);
            }
            foreach ($stages as $stage) {
                if ($stage instanceof DeduplicateStage) {
                    $rejectedIndices = array_map(fn (RecordFailure $failure): int|string => $failure->index(), $failures);
                    foreach ($processed->records() as $record) {
                        if ($record->isDropped()) {
                            $rejectedIndices[] = $record->index();
                        }
                    }
                    $stage->rollbackClaims($strictFailure ? null : $rejectedIndices);
                    $stage->recordDurableClaims();
                }
            }
            $result = new ChunkExecutionResult($processed, $sinkResult->persistedCount, $failures);
            if ($recordCheckpoint && $repository !== null && $runId !== null) {
                $records = $processed->records();
                $last = $records !== [] ? $records[array_key_last($records)] : null;
                $status = $result->failed() === 0 ? PipelineStatus::Completed : ($result->successful > 0 ? PipelineStatus::Partial : PipelineStatus::Failed);
                $checkpoint = $repository->recordChunk($runId, $processed->sequence(), $status->value, $processed->count(), $result->successful, $result->failed(), $last !== null && is_numeric($last->index()) ? (int) $last->index() : null);
                $repository->reconcileChunkErrors($runId, $checkpoint->id, $failures, $redactedAttributes);
                $repository->updateRunProgress($runId, $processed->count(), $result->successful, $result->failed());
            }

            if ($afterPersistence !== null) {
                $afterPersistence($result);
            }

            return $result;
        };
        try {
            if ($transactional) {
                $connection = $repository !== null ? $tracking : $destination;
                $result = DB::connection($connection)->transaction(function () use ($work, $destination, $connection, $sink): ChunkExecutionResult {
                    return $sink !== null && $destination !== $connection
                        ? DB::connection($destination)->transaction($work)
                        : $work();
                });
            } else {
                $result = $work();
            }
        } catch (Throwable $exception) {
            foreach ($stages as $stage) {
                if ($stage instanceof DeduplicateStage) {
                    try {
                        $stage->rollbackClaims();
                    } catch (Throwable $rollbackException) {
                        report($rollbackException);
                    }
                }
            }
            throw $exception;
        }
        $commitConnection = DB::connection($repository !== null ? $tracking : $destination);
        foreach ($stages as $stage) {
            if ($stage instanceof DeduplicateStage) {
                $commit = function () use ($stage): void {
                    try {
                        $stage->commitClaims();
                    } catch (Throwable $cacheException) {
                        report($cacheException);
                    }
                };
                if ($commitConnection->transactionLevel() > 0) {
                    $commitConnection->afterCommit($commit);
                } else {
                    $commit();
                }
            }
        }
        foreach ($result->failures as $failure) {
            $notify = function () use ($runId, $failure): void {
                try {
                    PipelineRecordFailed::dispatch($runId, $failure);
                } catch (Throwable $listenerException) {
                    report($listenerException);
                }
            };
            if ($commitConnection->transactionLevel() > 0) {
                $commitConnection->afterCommit($notify);
            } else {
                $notify();
            }
        }

        return $result;
    }

    /** @param array<int, RecordStageInterface|ChunkStageInterface> $stages */
    public function processStages(RecordChunk $chunk, array $stages, ?string $runId = null): RecordChunk
    {
        foreach ($stages as $position => $stage) {
            if ($stage instanceof DeduplicateStage) {
                if ($stage->isChunkLocal()) {
                    $stage->reset();
                }
                $stage->prepareChunk($chunk, $runId, $position);
            }
            if ($stage instanceof RecordStageInterface) {
                foreach ($chunk->records() as $record) {
                    if (! $record->isDropped()) {
                        $stage->handleRecord($record);
                    }
                }
            } else {
                $chunk = $stage->handleChunk($chunk);
            }
        }

        return $chunk;
    }

    /** @param array<int, RecordContext> $candidates */
    private function attributeSinkFailures(array $candidates, SinkResult $result): void
    {
        $failed = array_flip($result->failedIndices ?: ($result->metadata['failed_indices'] ?? []));
        $persisted = array_flip($result->persistedIndices ?: ($result->metadata['persisted_indices'] ?? []));
        if ($result->persistedCount < 0 || $result->persistedCount > count($candidates) || $result->failedCount < 0) {
            throw new LogicException('Sink returned invalid record counts.');
        }
        foreach ($candidates as $record) {
            $index = $record->index();
            $identifier = $record->identifier();
            if (isset($failed[$index]) || ($identifier !== null && isset($failed[$identifier])) || ($persisted !== [] && ! isset($persisted[$index]) && ($identifier === null || ! isset($persisted[$identifier]))) || ($result->persistedCount === 0)) {
                $record->addError('_sink', $result->metadata['exception'] ?? ($result->metadata['error'] ?? 'Sink persistence failed.'));
            }
        }
        $invalidCount = count(array_filter($candidates, fn (RecordContext $record): bool => $record->isInvalid()));
        if ($result->persistedCount + $invalidCount !== count($candidates)) {
            throw new LogicException('Partial sink results must identify persisted or failed records.');
        }
    }
}
