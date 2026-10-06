<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Queue;

use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Database\UniqueConstraintViolationException;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\Contracts\ChunkStageInterface;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Events\PipelineChunkCompleted;
use Robel\DataPipeline\Events\PipelineFailed;
use Robel\DataPipeline\Events\PipelineRecordFailed;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\RecordErrorBag;
use Robel\DataPipeline\Results\RecordFailure;
use Robel\DataPipeline\Stages\DeduplicateStage;

class ProcessPipelineChunkJob implements ShouldQueue
{
    use Batchable, Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * @param  array<int, array{index: int|string, raw: array<string, mixed>}>  $rawRecords
     * @param  array<int, RecordStageInterface|ChunkStageInterface>  $stages
     * @param  array<int, string>  $redactedAttributes
     */
    public function __construct(
        public string $runId,
        public int $sequence,
        public array $rawRecords,
        public ?string $pipelineDefinition = null,
        public array $stages = [],
        public ?SinkInterface $sink = null,
        public array $redactedAttributes = [],
        public bool $allowPartialSuccess = true,
        public bool $transactional = true,
        public int $throttleMs = 0,
        public ?SerializableClosure $progressCallback = null,
        public bool $allowCrossConnectionTransactions = false,
    ) {}

    public function handle(PipelineRepositoryInterface $repository): void
    {
        if ($this->batch()?->cancelled()) {
            return;
        }

        // 1. Atomic chunk lease / concurrency guard
        $isClaimed = DB::transaction(function () {
            $existingChunk = PipelineChunk::where('pipeline_id', $this->runId)
                ->where('sequence', $this->sequence)
                ->lockForUpdate()
                ->first();

            if ($existingChunk !== null) {
                if (in_array($existingChunk->status, ['COMPLETED', 'PARTIAL'], true)) {
                    return false;
                }

                if ($existingChunk->status === 'PROCESSING') {
                    $isStale = $existingChunk->started_at !== null && $existingChunk->started_at->diffInSeconds(now()) >= 300;

                    if (! $isStale) {
                        return false;
                    }
                }
            }

            if ($existingChunk === null) {
                try {
                    PipelineChunk::create([
                        'pipeline_id' => $this->runId,
                        'sequence' => $this->sequence,
                        'status' => 'PROCESSING',
                        'records_count' => count($this->rawRecords),
                        'started_at' => now(),
                    ]);
                } catch (UniqueConstraintViolationException) {
                    return false;
                }
            } else {
                $existingChunk->update([
                    'status' => 'PROCESSING',
                    'started_at' => now(),
                ]);
            }

            return true;
        });

        if (! $isClaimed) {
            return;
        }

        if ($this->throttleMs > 0) {
            usleep($this->throttleMs * 1000);
        }

        $startTime = microtime(true);
        $isCommitted = false;

        try {
            // 2. Resolve stages and sink from definition if provided
            $stages = $this->stages;
            $sink = $this->sink;

            if (! empty($this->pipelineDefinition)) {
                $resolved = DataPipeline::resolveDefinition($this->pipelineDefinition);
                if ($resolved === null) {
                    PipelineChunk::where('pipeline_id', $this->runId)
                        ->where('sequence', $this->sequence)
                        ->update(['status' => 'FAILED']);

                    throw new \RuntimeException(
                        "Cannot process pipeline chunk #{$this->sequence} for run [{$this->runId}]: Pipeline definition '{$this->pipelineDefinition}' could not be resolved."
                    );
                }
                $stages = $resolved->stages();
                $sink = $resolved->sink();
            }

            $stages = array_map(function ($stage) {
                $cloned = is_object($stage) ? clone $stage : $stage;
                if ($cloned instanceof DeduplicateStage && ! $cloned->isChunkLocal() && $cloned->getStore() === null) {
                    $cloned = $cloned->withStore(config('cache.default') ?? 'default', "pipeline:dedup:{$this->runId}:");
                }
                if (is_object($cloned) && method_exists($cloned, 'reset')) {
                    $cloned->reset();
                }

                return $cloned;
            }, $stages);

            // 3. Reconstruct record contexts
            $recordContexts = [];
            foreach ($this->rawRecords as $item) {
                $recordContexts[] = RecordContext::fromRaw($item['raw'], $item['index']);
            }

            $chunk = new RecordChunk($recordContexts, $this->sequence);

            // 4. Execute stages preserving exact fluent registration order
            foreach ($stages as $stage) {
                if ($stage instanceof RecordStageInterface) {
                    foreach ($chunk->records() as $record) {
                        if (! $record->isDropped()) {
                            $stage->handleRecord($record);
                        }
                    }
                } elseif ($stage instanceof ChunkStageInterface) {
                    $chunk = $stage->handleChunk($chunk);
                }
            }

            // 5. Partition failures
            $chunkFailures = [];
            foreach ($chunk->invalid() as $invalidRecord) {
                $failure = new RecordFailure(
                    index: $invalidRecord->index(),
                    identifier: $invalidRecord->identifier(),
                    rawInput: $invalidRecord->raw(),
                    errors: $invalidRecord->errors(),
                );
                $chunkFailures[] = $failure;
                PipelineRecordFailed::dispatch($this->runId, $failure);
            }

            $chunkSuccessful = 0;
            $chunkFailed = count($chunkFailures);

            // Strict failure: fail immediately and roll back/skip sink persistence
            if (! $this->allowPartialSuccess && $chunkFailed > 0) {
                foreach ($stages as $stage) {
                    if ($stage instanceof DeduplicateStage) {
                        $stage->rollbackClaims();
                    }
                }

                $records = $chunk->records();
                $lastRecord = ! empty($records) ? $records[count($records) - 1] : null;
                $lastOffset = $lastRecord && is_numeric($lastRecord->index()) ? (int) $lastRecord->index() : null;

                $chunkRecord = $repository->recordChunk(
                    runId: $this->runId,
                    sequence: $this->sequence,
                    status: 'FAILED',
                    recordsCount: $chunk->count(),
                    successfulCount: 0,
                    failedCount: $chunkFailed,
                    lastOffset: $lastOffset,
                );

                $repository->reconcileChunkErrors(
                    runId: $this->runId,
                    chunkId: $chunkRecord->id,
                    failures: $chunkFailures,
                    redactedAttributes: $this->redactedAttributes,
                );

                $repository->updateRunProgress(
                    runId: $this->runId,
                    processedIncrement: $chunk->count(),
                    successfulIncrement: 0,
                    failedIncrement: $chunkFailed,
                );

                $errorBag = new RecordErrorBag;
                foreach ($chunkFailures as $cf) {
                    $errorBag->add($cf);
                }

                $validationException = new PipelineValidationException(
                    "Pipeline chunk #{$this->sequence} failed strict validation: {$chunkFailed} records contained errors.",
                    $errorBag
                );

                $repository->markRunFailed(
                    $this->runId,
                    "Pipeline execution failed: {$chunkFailed} records contained errors under strict failIfAny policy."
                );

                PipelineFailed::dispatch($this->runId, $validationException);

                $this->batch()?->cancel();

                return;
            }

            $processPersistence = function () use (
                $chunk,
                $sink,
                &$chunkSuccessful,
                &$chunkFailed,
                &$chunkFailures,
                $repository,
                $stages,
            ) {
                // 6. Persist to Sink
                if ($sink !== null) {
                    $sinkResult = $sink->persist($chunk);
                    $chunkSuccessful += $sinkResult->persistedCount;

                    if ($sinkResult->persistedCount === 0 && $sinkResult->failedCount > 0) {
                        foreach ($stages as $stage) {
                            if ($stage instanceof DeduplicateStage) {
                                $stage->rollbackClaims();
                            }
                        }
                    } elseif ($sinkResult->failedCount > 0) {
                        $failedIndices = array_map(fn ($f) => $f->index(), $chunkFailures);
                        foreach ($stages as $stage) {
                            if ($stage instanceof DeduplicateStage) {
                                $stage->rollbackClaims($failedIndices);
                                $stage->commitClaims();
                            }
                        }
                    } else {
                        foreach ($stages as $stage) {
                            if ($stage instanceof DeduplicateStage) {
                                $stage->commitClaims();
                            }
                        }
                    }

                    if ($sinkResult->failedCount > 0) {
                        $chunkFailed += $sinkResult->failedCount;
                        $alreadyFailedIndices = array_flip(array_map(fn ($f) => $f->index(), $chunkFailures));

                        // Check ALL chunk records for sink errors
                        foreach ($chunk->records() as $r) {
                            if ($r->isInvalid() && ! isset($alreadyFailedIndices[$r->index()])) {
                                $failure = new RecordFailure(
                                    index: $r->index(),
                                    identifier: $r->identifier(),
                                    rawInput: $r->raw(),
                                    errors: $r->errors(),
                                );
                                $chunkFailures[] = $failure;
                                $alreadyFailedIndices[$r->index()] = true;
                                PipelineRecordFailed::dispatch($this->runId, $failure);
                            }
                        }

                        if (count($chunkFailures) < $chunkFailed) {
                            foreach ($chunk->records() as $r) {
                                if (! isset($alreadyFailedIndices[$r->index()])) {
                                    $r->addError('_sink', $sinkResult->metadata['exception'] ?? ($sinkResult->metadata['error'] ?? 'Sink persistence failed.'));
                                    $failure = new RecordFailure(
                                        index: $r->index(),
                                        identifier: $r->identifier(),
                                        rawInput: $r->raw(),
                                        errors: $r->errors(),
                                    );
                                    $chunkFailures[] = $failure;
                                    $alreadyFailedIndices[$r->index()] = true;
                                    PipelineRecordFailed::dispatch($this->runId, $failure);
                                }
                            }
                        }

                        if (! $this->allowPartialSuccess) {
                            $errBag = new RecordErrorBag;
                            foreach ($chunkFailures as $cf) {
                                $errBag->add($cf);
                            }

                            throw new PipelineValidationException(
                                "Pipeline chunk #{$this->sequence} failed sink persistence for {$sinkResult->failedCount} records.",
                                $errBag
                            );
                        }
                    }
                } else {
                    $chunkSuccessful += count($chunk->valid());

                    foreach ($stages as $stage) {
                        if ($stage instanceof DeduplicateStage) {
                            $stage->commitClaims();
                        }
                    }
                }

                // 7. Record checkpoint and quarantine
                $records = $chunk->records();
                $lastRecord = ! empty($records) ? $records[count($records) - 1] : null;
                $lastOffset = $lastRecord && is_numeric($lastRecord->index()) ? (int) $lastRecord->index() : null;

                $chunkStatus = match (true) {
                    $chunkFailed === 0 => 'COMPLETED',
                    $chunkSuccessful > 0 => 'PARTIAL',
                    default => 'FAILED',
                };

                $chunkRecord = $repository->recordChunk(
                    runId: $this->runId,
                    sequence: $this->sequence,
                    status: $chunkStatus,
                    recordsCount: $chunk->count(),
                    successfulCount: $chunkSuccessful,
                    failedCount: $chunkFailed,
                    lastOffset: $lastOffset,
                );

                $repository->reconcileChunkErrors(
                    runId: $this->runId,
                    chunkId: $chunkRecord->id,
                    failures: $chunkFailures,
                    redactedAttributes: $this->redactedAttributes,
                );

                $repository->updateRunProgress(
                    runId: $this->runId,
                    processedIncrement: $chunk->count(),
                    successfulIncrement: $chunkSuccessful,
                    failedIncrement: $chunkFailed,
                );
            };

            $isCommitted = false;

            $destConnection = ($sink !== null && method_exists($sink, 'getConnection'))
                ? $sink->getConnection()
                : null;
            $trackingConnection = config('database.default');

            if ($this->transactional && $sink !== null) {
                if ($destConnection !== null && $destConnection !== $trackingConnection) {
                    if (! $this->allowCrossConnectionTransactions) {
                        throw new \LogicException(
                            "Transactional pipeline execution cannot guarantee dual-connection atomicity across destination connection '{$destConnection}' and tracking connection '{$trackingConnection}'. Either configure both to use the same connection, or call ->allowCrossConnectionTransactions() to explicitly proceed with nested commits."
                        );
                    }

                    DB::connection($trackingConnection)->transaction(function () use ($destConnection, $processPersistence) {
                        DB::connection($destConnection)->transaction(function () use ($processPersistence) {
                            $processPersistence();
                        });
                    });
                } else {
                    $conn = $destConnection ?? $trackingConnection;
                    DB::connection($conn)->transaction($processPersistence);
                }
            } else {
                $processPersistence();
            }

            $isCommitted = true;

            $duration = round(microtime(true) - $startTime, 4);

            if ($this->progressCallback !== null) {
                try {
                    $runModel = $repository->findRun($this->runId);
                    ($this->progressCallback->getClosure())(
                        $runModel?->processed_records ?? count($this->rawRecords),
                        $runModel?->total_records,
                        $chunkSuccessful,
                        $chunkFailed,
                        $chunk,
                    );
                } catch (\Throwable $progressEx) {
                    report($progressEx);
                }
            }

            try {
                PipelineChunkCompleted::dispatch(
                    $this->runId,
                    $this->sequence,
                    $chunk->count(),
                    $chunkSuccessful,
                    $chunkFailed,
                    $duration,
                );
            } catch (\Throwable $listenerException) {
                report($listenerException);
            }
        } catch (\Throwable $e) {
            foreach ($stages as $stage) {
                if ($stage instanceof DeduplicateStage) {
                    $stage->rollbackClaims();
                }
            }

            if (! $isCommitted) {
                PipelineChunk::where('pipeline_id', $this->runId)
                    ->where('sequence', $this->sequence)
                    ->update([
                        'status' => 'FAILED',
                        'completed_at' => now(),
                    ]);
            }

            if (! $this->allowPartialSuccess) {
                $repository->markRunFailed($this->runId, $e->getMessage());
                PipelineFailed::dispatch($this->runId, $e);
            }

            throw $e;
        }
    }

    public function failed(?\Throwable $exception = null): void
    {
        $chunk = PipelineChunk::where('pipeline_id', $this->runId)
            ->where('sequence', $this->sequence)
            ->first();

        if ($chunk !== null && ! in_array($chunk->status, ['COMPLETED', 'PARTIAL'], true)) {
            $chunk->update([
                'status' => 'FAILED',
                'completed_at' => now(),
            ]);
        }

        if (! $this->allowPartialSuccess && $exception !== null) {
            app(PipelineRepositoryInterface::class)->markRunFailed($this->runId, $exception->getMessage());
            PipelineFailed::dispatch($this->runId, $exception);
        }
    }
}
