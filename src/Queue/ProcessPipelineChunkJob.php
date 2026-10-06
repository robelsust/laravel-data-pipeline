<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Queue;

use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\ChunkExecutor;
use Robel\DataPipeline\Contracts\ChunkStageInterface;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Events\PipelineChunkCompleted;
use Robel\DataPipeline\Events\PipelineFailed;
use Robel\DataPipeline\Exceptions\PersistenceError;
use Robel\DataPipeline\Exceptions\PipelineBusyException;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\PipelineStatus;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\RecordErrorBag;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Throwable;

class ProcessPipelineChunkJob implements ShouldQueue
{
    use Batchable, Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries;

    public int $timeout;

    /** @var array<int, string> */
    public array $redactedAttributes;

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
        ?array $redactedAttributes = null,
        public bool $allowPartialSuccess = true,
        public bool $transactional = true,
        public int $throttleMs = 0,
        public ?SerializableClosure $progressCallback = null,
        public bool $allowCrossConnectionTransactions = false,
    ) {
        $this->tries = max(1, (int) config('data-pipeline.queue.max_retries', 30));
        $this->timeout = max(1, (int) config('data-pipeline.queue.timeout', 60));
        $this->redactedAttributes = $redactedAttributes ?? config('data-pipeline.redacted_attributes', []);
    }

    /** @return array<int, int> */
    public function backoff(): array
    {
        return [5, 15, 30, 60];
    }

    public function handle(PipelineRepositoryInterface $repository): void
    {
        if ($this->batch()?->cancelled()) {
            return;
        }
        $connection = DB::connection(config('data-pipeline.connection'));
        $identity = ['pipeline_id' => $this->runId, 'sequence' => $this->sequence];
        $connection->table('pipeline_chunks')->insertOrIgnore($identity + ['status' => PipelineStatus::Pending->value, 'records_count' => count($this->rawRecords)]);
        $token = (string) Str::uuid();
        $leaseSeconds = max($this->timeout + 30, (int) config('data-pipeline.queue.lease_seconds', 90));
        $claimed = $connection->table('pipeline_chunks')->where($identity)
            ->where(function ($query) use ($leaseSeconds): void {
                $query->whereIn('status', [PipelineStatus::Pending->value, PipelineStatus::Failed->value])
                    ->orWhere(function ($query) use ($leaseSeconds): void {
                        $query->where('status', PipelineStatus::Processing->value)
                            ->where(function ($query) use ($leaseSeconds): void {
                                $query->where('started_at', '<=', now()->subSeconds($leaseSeconds))->orWhereNull('started_at');
                            });
                    });
            })->update(['status' => PipelineStatus::Processing->value, 'started_at' => now(), 'lease_token' => $token]);
        if ($claimed === 0) {
            $status = $connection->table('pipeline_chunks')->where($identity)->value('status');
            if (! in_array($status, [PipelineStatus::Completed->value, PipelineStatus::Partial->value], true)) {
                $this->waitForOwner();
            }

            return;
        }
        $started = microtime(true);
        $stages = [];
        try {
            if ($this->throttleMs > 0) {
                usleep($this->throttleMs * 1000);
            }
            $stages = $this->stages;
            $sink = $this->sink;
            if ($this->pipelineDefinition !== null) {
                $definition = DataPipeline::resolveDefinition($this->pipelineDefinition);
                if ($definition === null) {
                    throw new \RuntimeException("Cannot process pipeline chunk #{$this->sequence} for run [{$this->runId}]: Pipeline definition '{$this->pipelineDefinition}' could not be resolved.");
                }
                $stages = $definition->stages();
                $sink = $definition->sink();
            }
            $stages = array_map(function (RecordStageInterface|ChunkStageInterface $stage): RecordStageInterface|ChunkStageInterface {
                $stage = clone $stage;
                if ($stage instanceof DeduplicateStage && ! $stage->isChunkLocal() && $stage->getStore() === null) {
                    $stage = $stage->withStore(config('cache.default'), "pipeline:dedup:{$this->runId}:");
                }
                if (method_exists($stage, 'reset')) {
                    $stage->reset();
                }

                return $stage;
            }, $stages);
            $records = array_map(fn (array $row): RecordContext => RecordContext::fromRaw($row['raw'], $row['index']), $this->rawRecords);
            $result = app(ChunkExecutor::class)->execute(new RecordChunk($records, $this->sequence), $stages, $sink, $repository, $this->runId, $this->allowPartialSuccess, $this->transactional, $this->allowCrossConnectionTransactions, $this->redactedAttributes, $token);
        } catch (Throwable $exception) {
            $connection->table('pipeline_chunks')->where($identity)->where('lease_token', $token)->update(['status' => PipelineStatus::Failed->value, 'completed_at' => now()]);
            if ($exception instanceof PipelineBusyException) {
                $this->waitForOwner();

                return;
            }
            if (! $this->allowPartialSuccess) {
                $repository->markRunFailed($this->runId, PersistenceError::describe($exception));
                PipelineFailed::dispatch($this->runId, $exception);
            }
            throw $exception;
        }
        if (! $this->allowPartialSuccess && $result->failed() > 0) {
            $bag = new RecordErrorBag;
            foreach ($result->failures as $failure) {
                $bag->add($failure);
            }
            $exception = new PipelineValidationException('Pipeline chunk failed strict validation under failIfAny policy.', $bag);
            $repository->markRunFailed($this->runId, PersistenceError::describe($exception));
            PipelineFailed::dispatch($this->runId, $exception);
            $this->batch()?->cancel();

            return;
        }
        try {
            if ($this->progressCallback !== null) {
                $run = $repository->findRun($this->runId);
                ($this->progressCallback->getClosure())($run?->processed_records, $run?->total_records, $result->successful, $result->failed(), $result->chunk);
            }
            PipelineChunkCompleted::dispatch($this->runId, $this->sequence, $result->chunk->count(), $result->successful, $result->failed(), round(microtime(true) - $started, 4));
        } catch (Throwable $listenerException) {
            report($listenerException);
        }
    }

    private function waitForOwner(): void
    {
        if ($this->job === null) {
            throw new PipelineBusyException('Pipeline chunk is owned by an active worker; retry after its lease expires.');
        }
        $this->release(max(1, (int) config('data-pipeline.queue.release_delay', 5)));
    }

    public function failed(?Throwable $exception = null): void
    {
        if ($exception instanceof PipelineBusyException) {
            return;
        }
        $leaseSeconds = max($this->timeout + 30, (int) config('data-pipeline.queue.lease_seconds', 90));
        $changed = PipelineChunk::where('pipeline_id', $this->runId)->where('sequence', $this->sequence)
            ->where(function ($query) use ($leaseSeconds): void {
                $query->whereIn('status', [PipelineStatus::Pending->value, PipelineStatus::Failed->value])
                    ->orWhere(function ($query) use ($leaseSeconds): void {
                        $query->where('status', PipelineStatus::Processing->value)
                            ->where('started_at', '<=', now()->subSeconds($leaseSeconds));
                    });
            })->update(['status' => PipelineStatus::Failed->value, 'completed_at' => now()]);
        if ($changed > 0 && $exception !== null) {
            app(PipelineRepositoryInterface::class)->markRunFailed($this->runId, PersistenceError::describe($exception));
            PipelineFailed::dispatch($this->runId, $exception);
        }
    }
}
