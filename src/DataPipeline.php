<?php

declare(strict_types=1);

namespace Robel\DataPipeline;

use Closure;
use Generator;
use Illuminate\Bus\Batch;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\LazyCollection;
use InvalidArgumentException;
use Laravel\SerializableClosure\SerializableClosure;
use ReflectionClass;
use Robel\DataPipeline\Contracts\ChunkStageInterface;
use Robel\DataPipeline\Contracts\PipelineDefinitionInterface;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\Contracts\SourceInterface;
use Robel\DataPipeline\Events\PipelineChunkCompleted;
use Robel\DataPipeline\Events\PipelineCompleted;
use Robel\DataPipeline\Events\PipelineFailed;
use Robel\DataPipeline\Events\PipelineStarted;
use Robel\DataPipeline\Exceptions\PersistenceError;
use Robel\DataPipeline\Exceptions\PipelineBusyException;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\ChunkExecutionResult;
use Robel\DataPipeline\Results\PipelineResult;
use Robel\DataPipeline\Results\RecordErrorBag;
use Robel\DataPipeline\Results\RecordFailure;
use Robel\DataPipeline\Sinks\CustomSink;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Sinks\UpsertSink;
use Robel\DataPipeline\Sources\ArraySource;
use Robel\DataPipeline\Sources\CsvSource;
use Robel\DataPipeline\Sources\LazySource;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Robel\DataPipeline\Stages\FilterStage;
use Robel\DataPipeline\Stages\MapFieldsStage;
use Robel\DataPipeline\Stages\RejectRedactedPayloadStage;
use Robel\DataPipeline\Stages\ResolveRelationStage;
use Robel\DataPipeline\Stages\TransformStage;
use Robel\DataPipeline\Stages\ValidateStage;
use Throwable;
use Traversable;

/** @phpstan-consistent-constructor */
class DataPipeline
{
    /**
     * Registered named pipeline definitions.
     *
     * @var array<string, Closure|class-string<PipelineDefinitionInterface>>
     */
    private static array $definitions = [];

    private ?string $definitionKey = null;

    private SourceInterface $source;

    private int $chunkSize = 500;

    private array $stages = [];

    private ?SinkInterface $sink = null;

    private bool $allowPartialSuccess = true;

    private ?string $name = null;

    private bool $tracked = false;

    private array $redactedAttributes = [];

    private ?string $runId = null;

    private ?string $queueName = null;

    private ?string $connectionName = null;

    private ?Closure $progressCallback = null;

    private bool $transactional = true;

    private int $throttleMs = 0;

    private bool $allowCrossConnectionTransactions = false;

    public function __construct(mixed $source)
    {
        $this->source = $this->resolveSource($source);
        $this->chunkSize = max(1, (int) config('data-pipeline.default_chunk_size', 500));
        $this->redactedAttributes = config('data-pipeline.redacted_attributes', []);
        $this->queueName = config('data-pipeline.queue.name', 'default');
        $this->connectionName = config('data-pipeline.queue.connection');
    }

    public static function register(string $name, Closure|string $definition): void
    {
        self::$definitions[$name] = $definition;
    }

    public static function resolveDefinition(string $name): ?static
    {
        if (isset(self::$definitions[$name])) {
            $def = self::$definitions[$name];
            if ($def instanceof Closure) {
                return $def();
            }
            if (class_exists($def)) {
                return app($def)->pipeline();
            }
        }

        if (class_exists($name) && is_subclass_of($name, PipelineDefinitionInterface::class)) {
            return app($name)->pipeline();
        }

        return null;
    }

    public function as(string $definitionKey): static
    {
        $this->definitionKey = $definitionKey;
        if ($this->name === null) {
            $this->name = $definitionKey;
        }

        return $this;
    }

    public function stages(): array
    {
        return $this->stages;
    }

    public function sink(): ?SinkInterface
    {
        return $this->sink;
    }

    public function hasClosuresInStages(): bool
    {
        foreach ($this->stages as $stage) {
            if ($stage instanceof TransformStage || $stage instanceof FilterStage) {
                return true;
            }
            if ($stage instanceof DeduplicateStage) {
                $ref = new ReflectionClass($stage);
                $keyProp = $ref->getProperty('key');
                if ($keyProp->getValue($stage) instanceof Closure) {
                    return true;
                }
            }
            if ($stage instanceof ResolveRelationStage) {
                $ref = new ReflectionClass($stage);
                $autoProp = $ref->getProperty('autoCreate');
                if ($autoProp->getValue($stage) !== null) {
                    return true;
                }
            }
        }

        if ($this->sink instanceof CustomSink) {
            return true;
        }

        return false;
    }

    /**
     * Fluent entry point.
     */
    public static function from(mixed $source): static
    {
        return new static($source);
    }

    public function name(string $name): static
    {
        $this->name = $name;

        return $this;
    }

    public function track(bool $track = true): static
    {
        $this->tracked = $track;

        return $this;
    }

    public function tracked(bool $track = true): static
    {
        return $this->track($track);
    }

    public function redactAttributes(array $attributes): static
    {
        $this->redactedAttributes = $attributes;

        return $this;
    }

    public function chunk(int $size): static
    {
        if ($size < 1) {
            throw new InvalidArgumentException('Chunk size must be at least 1.');
        }

        $this->chunkSize = $size;

        return $this;
    }

    public function mapFields(array $mapping): static
    {
        return $this->pipe(new MapFieldsStage($mapping));
    }

    public function transform(Closure|array $transformer): static
    {
        return $this->pipe(new TransformStage($transformer));
    }

    public function map(Closure|array $transformer): static
    {
        return $this->transform($transformer);
    }

    public function filter(Closure $predicate): static
    {
        return $this->pipe(new FilterStage($predicate));
    }

    public function deduplicate(
        string|array|Closure $key,
        string $action = 'drop',
        ?string $store = null,
        ?string $scope = null,
        int $maxKeys = 50000,
        bool $chunkLocal = false
    ): static {
        return $this->deduplicateBy($key, $action, $store, $scope, $maxKeys, $chunkLocal);
    }

    public function deduplicateBy(
        string|array|Closure $key,
        string $action = 'drop',
        ?string $store = null,
        ?string $scope = null,
        int $maxKeys = 50000,
        bool $chunkLocal = false
    ): static {
        return $this->pipe(new DeduplicateStage(
            key: $key,
            action: $action,
            store: $store,
            scope: $scope,
            maxKeys: $maxKeys,
            chunkLocal: $chunkLocal
        ));
    }

    public function deduplicatePerChunk(
        string|array|Closure $key,
        string $action = 'drop',
        int $maxKeys = 50000
    ): static {
        return $this->deduplicateBy($key, action: $action, maxKeys: $maxKeys, chunkLocal: true);
    }

    public function onProgress(Closure $callback): static
    {
        $this->progressCallback = $callback;

        return $this;
    }

    public function transactional(bool $enabled = true): static
    {
        $this->transactional = $enabled;

        return $this;
    }

    public function allowCrossConnectionTransactions(bool $allow = true): static
    {
        $this->allowCrossConnectionTransactions = $allow;

        return $this;
    }

    public function throttle(int $sleepMilliseconds): static
    {
        $this->throttleMs = max(0, $sleepMilliseconds);

        return $this;
    }

    public function validate(array $rules, array $messages = [], array $customAttributes = []): static
    {
        return $this->pipe(new ValidateStage($rules, $messages, $customAttributes));
    }

    public function resolveRelation(
        string $foreignKey,
        string $lookupColumn,
        string $target,
        string $targetKey = 'name',
        string $primaryKey = 'id',
        bool $required = true,
        ?Closure $autoCreate = null,
    ): static {
        return $this->pipe(new ResolveRelationStage(
            foreignKey: $foreignKey,
            lookupColumn: $lookupColumn,
            target: $target,
            targetKey: $targetKey,
            primaryKey: $primaryKey,
            required: $required,
            autoCreate: $autoCreate,
        ));
    }

    public function pipe(RecordStageInterface|ChunkStageInterface $stage): static
    {
        $this->stages[] = $stage;

        return $this;
    }

    public function allowPartialSuccess(bool $allow = true): static
    {
        $this->allowPartialSuccess = $allow;

        return $this;
    }

    public function failIfAny(): static
    {
        $this->allowPartialSuccess = false;

        return $this;
    }

    public function insert(string $tableOrModel): static
    {
        $this->sink = new InsertSink($tableOrModel);

        return $this;
    }

    public function upsert(string $tableOrModel, array|string $uniqueBy, ?array $update = null): static
    {
        $this->sink = new UpsertSink($tableOrModel, $uniqueBy, $update);

        return $this;
    }

    public function persist(Closure $callback): static
    {
        $this->sink = new CustomSink($callback);

        return $this;
    }

    public function to(SinkInterface $sink): static
    {
        $this->sink = $sink;

        return $this;
    }

    /**
     * Preview the transformed, validated state of the first N records without persisting to sink.
     * Guaranteed zero database writes via transaction rollback and preview chunk flag.
     *
     * @return Collection<int, RecordContext>
     */
    /**
     * Preview the transformed, validated state of the first N records without persisting to sink.
     * Guaranteed zero database writes via transaction rollback and preview chunk flag.
     *
     * @return Collection<int, RecordContext>
     */
    public function preview(int $limit = 10): Collection
    {
        DB::beginTransaction();

        try {
            $results = collect();
            $recordsBuffer = [];
            $count = 0;

            foreach ($this->source->records() as $index => $raw) {
                $recordsBuffer[] = RecordContext::fromRaw($raw, $index, isPreview: true);
                $count++;

                if ($count >= $limit) {
                    break;
                }
            }

            if (empty($recordsBuffer)) {
                return $results;
            }

            $chunk = new RecordChunk($recordsBuffer, 1, isDryRun: false, isPreview: true);
            $previewStages = $this->freshStages();
            $processedChunk = $this->executeStagesOnChunk($chunk, $previewStages);

            return collect($processedChunk->records());
        } finally {
            DB::rollBack();
        }
    }

    /**
     * Perform a dry-run simulation across the source without mutating the sink.
     * Guaranteed zero database writes via transaction rollback and dry-run chunk flag.
     *
     * @return array{total: int, valid: int, invalid: int, dropped: int, errors: RecordErrorBag}
     */
    public function dryRun(): array
    {
        DB::beginTransaction();

        try {
            $total = 0;
            $validCount = 0;
            $invalidCount = 0;
            $droppedCount = 0;
            $errorBag = new RecordErrorBag;

            $chunkSequence = 1;
            $currentChunk = [];
            $dryRunStages = $this->freshStages();

            foreach ($this->source->records() as $index => $raw) {
                $total++;
                $currentChunk[] = RecordContext::fromRaw($raw, $index, isDryRun: true);

                if (count($currentChunk) >= $this->chunkSize) {
                    $chunk = new RecordChunk($currentChunk, $chunkSequence++, isDryRun: true, isPreview: false);
                    $processed = $this->executeStagesOnChunk($chunk, $dryRunStages);

                    $validCount += count($processed->valid());
                    $invalidCount += count($processed->invalid());
                    $droppedCount += count($processed->dropped());

                    foreach ($processed->invalid() as $inv) {
                        $errorBag->add(new RecordFailure(
                            index: $inv->index(),
                            identifier: $inv->identifier(),
                            rawInput: $inv->raw(),
                            errors: $inv->errors(),
                        ));
                    }

                    $currentChunk = [];
                }
            }

            if (! empty($currentChunk)) {
                $chunk = new RecordChunk($currentChunk, $chunkSequence, isDryRun: true, isPreview: false);
                $processed = $this->executeStagesOnChunk($chunk, $dryRunStages);

                $validCount += count($processed->valid());
                $invalidCount += count($processed->invalid());
                $droppedCount += count($processed->dropped());

                foreach ($processed->invalid() as $inv) {
                    $errorBag->add(new RecordFailure(
                        index: $inv->index(),
                        identifier: $inv->identifier(),
                        rawInput: $inv->raw(),
                        errors: $inv->errors(),
                    ));
                }
            }

            return [
                'total' => $total,
                'valid' => $validCount,
                'invalid' => $invalidCount,
                'dropped' => $droppedCount,
                'errors' => $errorBag,
            ];
        } finally {
            DB::rollBack();
        }
    }

    /**
     * Execute the full pipeline synchronously.
     */
    public function run(): PipelineResult
    {
        $startTime = microtime(true);
        $totalProcessed = 0;
        $successful = 0;
        $failed = 0;
        $errorBag = new RecordErrorBag;

        $repository = $this->tracked
            ? app(PipelineRepositoryInterface::class)
            : null;

        if ($repository !== null) {
            $run = $repository->createRun(
                sourceType: class_basename($this->source),
                totalRecords: $this->source->count(),
                fingerprint: $this->source->fingerprint(),
                name: $this->name,
                metadata: array_filter([
                    'chunk_size' => $this->chunkSize,
                    'definition' => $this->definitionKey,
                ]),
            );
            $this->runId = $run->id;
        }

        PipelineStarted::dispatch($this->runId, $this->name, $this->source->count());

        $chunkSequence = 1;
        $currentChunk = [];
        $runStages = $this->freshStages();

        try {
            foreach ($this->source->records() as $index => $raw) {
                $totalProcessed++;
                $currentChunk[] = RecordContext::fromRaw($raw, $index);

                if (count($currentChunk) >= $this->chunkSize) {
                    $this->processChunk(
                        new RecordChunk($currentChunk, $chunkSequence++),
                        $totalProcessed,
                        $successful,
                        $failed,
                        $errorBag,
                        $repository,
                        $runStages
                    );
                    $currentChunk = [];

                    if ($this->throttleMs > 0) {
                        usleep($this->throttleMs * 1000);
                    }
                }
            }

            // Process trailing chunk
            if (! empty($currentChunk)) {
                $this->processChunk(
                    new RecordChunk($currentChunk, $chunkSequence),
                    $totalProcessed,
                    $successful,
                    $failed,
                    $errorBag,
                    $repository,
                    $runStages
                );
            }

            $duration = microtime(true) - $startTime;

            if ($repository !== null && $this->runId !== null) {
                $repository->markRunCompleted(
                    runId: $this->runId,
                    status: $failed > 0 ? PipelineStatus::PartialSuccess->value : PipelineStatus::Completed->value
                );
            }

            $result = new PipelineResult(
                totalProcessed: $totalProcessed,
                successful: $successful,
                failed: $failed,
                errors: $errorBag,
                durationInSeconds: round($duration, 4),
                metadata: array_filter(['name' => $this->name, 'run_id' => $this->runId]),
            );

            if (! $this->allowPartialSuccess && $failed > 0) {
                $validationException = new PipelineValidationException(
                    "Pipeline execution failed: {$failed} records contained errors.",
                    $errorBag
                );
                PipelineFailed::dispatch($this->runId, $validationException);

                throw $validationException;
            }

            PipelineCompleted::dispatch($this->runId, $result);

            return $result;
        } catch (Throwable $e) {
            if ($repository !== null && $this->runId !== null) {
                $repository->markRunFailed($this->runId, PersistenceError::describe($e));
            }

            if (! ($e instanceof PipelineValidationException)) {
                PipelineFailed::dispatch($this->runId, $e);
            }

            throw $e;
        }
    }

    public function onQueue(string $queue): static
    {
        $this->queueName = $queue;

        return $this;
    }

    public function onConnection(string $connection): static
    {
        $this->connectionName = $connection;

        return $this;
    }

    /**
     * Dispatch the pipeline across background queue workers using Laravel Bus::batch.
     */
    public function queue(): string
    {
        $repository = app(PipelineRepositoryInterface::class);

        $run = $repository->createRun(
            sourceType: class_basename($this->source),
            totalRecords: $this->source->count(),
            fingerprint: $this->source->fingerprint(),
            name: $this->name,
            metadata: array_filter([
                'chunk_size' => $this->chunkSize,
                'definition' => $this->definitionKey,
            ]),
        );

        $runId = $run->id;
        $this->runId = $runId;

        PipelineStarted::dispatch($runId, $this->name, $this->source->count());

        $chunkSequence = 1;
        $currentChunk = [];

        $serializableProgress = null;
        if ($this->progressCallback !== null) {
            try {
                $serializableProgress = new SerializableClosure($this->progressCallback);
                serialize($serializableProgress);
            } catch (Throwable $e) {
                throw new InvalidArgumentException(
                    'Progress callback cannot be serialized for queue workers. Use the PipelineChunkCompleted event listener instead for queued progress tracking.',
                    previous: $e
                );
            }
        }

        $allowPartial = $this->allowPartialSuccess;

        $batchBuilder = Bus::batch([])
            ->name($this->name ?? "Pipeline: {$runId}")
            ->then(function (Batch $batch) use ($runId, $allowPartial) {
                $repo = app(PipelineRepositoryInterface::class);
                $runModel = $repo->findRun($runId);
                $hasFailures = ($runModel && $runModel->failed_records > 0);

                if ($hasFailures && ! $allowPartial) {
                    $repo->markRunFailed($runId, "Pipeline execution failed: {$runModel->failed_records} records contained errors under strict failIfAny policy.");
                    PipelineFailed::dispatch($runId, new PipelineValidationException(
                        "Pipeline execution failed: {$runModel->failed_records} records contained errors under strict failIfAny policy.",
                        new RecordErrorBag
                    ));

                    return;
                }

                $status = $hasFailures ? PipelineStatus::PartialSuccess->value : PipelineStatus::Completed->value;
                $repo->markRunCompleted($runId, $status);

                PipelineCompleted::dispatch($runId, new PipelineResult(
                    totalProcessed: $runModel->processed_records ?? 0,
                    successful: $runModel->successful_records ?? 0,
                    failed: $runModel->failed_records ?? 0,
                    errors: new RecordErrorBag,
                    durationInSeconds: 0.0,
                    metadata: ['batch_id' => $batch->id],
                ));
            })
            ->catch(function (Batch $batch, Throwable $e) use ($runId) {
                $repo = app(PipelineRepositoryInterface::class);
                $repo->markRunFailed($runId, PersistenceError::describe($e));

                PipelineFailed::dispatch($runId, $e);
            });

        if ($this->queueName !== null) {
            $batchBuilder->onQueue($this->queueName);
        }

        if ($this->connectionName !== null) {
            $batchBuilder->onConnection($this->connectionName);
        }

        $queueStages = array_map(function ($stage) use ($runId) {
            if ($stage instanceof DeduplicateStage && ! $stage->isChunkLocal() && $stage->getStore() === null) {
                return $stage->withStore(config('cache.default') ?? 'default', "pipeline:dedup:{$runId}:");
            }

            return $stage;
        }, $this->stages);

        foreach ($this->source->records() as $index => $raw) {
            $currentChunk[] = ['index' => $index, 'raw' => $raw];

            if (count($currentChunk) >= $this->chunkSize) {
                $batchBuilder->add([
                    new ProcessPipelineChunkJob(
                        runId: $runId,
                        sequence: $chunkSequence++,
                        rawRecords: $currentChunk,
                        pipelineDefinition: $this->definitionKey,
                        stages: $this->definitionKey !== null ? [] : $queueStages,
                        sink: $this->definitionKey !== null ? null : $this->sink,
                        redactedAttributes: $this->redactedAttributes,
                        allowPartialSuccess: $this->allowPartialSuccess,
                        transactional: $this->transactional,
                        throttleMs: $this->throttleMs,
                        progressCallback: $serializableProgress,
                        allowCrossConnectionTransactions: $this->allowCrossConnectionTransactions,
                    ),
                ]);
                $currentChunk = [];
            }
        }

        if (! empty($currentChunk)) {
            $batchBuilder->add([
                new ProcessPipelineChunkJob(
                    runId: $runId,
                    sequence: $chunkSequence,
                    rawRecords: $currentChunk,
                    pipelineDefinition: $this->definitionKey,
                    stages: $this->definitionKey !== null ? [] : $queueStages,
                    sink: $this->definitionKey !== null ? null : $this->sink,
                    redactedAttributes: $this->redactedAttributes,
                    allowPartialSuccess: $this->allowPartialSuccess,
                    transactional: $this->transactional,
                    throttleMs: $this->throttleMs,
                    progressCallback: $serializableProgress,
                    allowCrossConnectionTransactions: $this->allowCrossConnectionTransactions,
                ),
            ]);
        }

        $batch = $batchBuilder->dispatch();

        $run->update([
            'metadata' => array_merge($run->metadata ?? [], ['batch_id' => $batch->id]),
        ]);

        return $runId;
    }

    /**
     * Resume an interrupted, paused, or failed data pipeline run from its uncommitted checkpoints.
     * Handles parallel execution gaps and committed PARTIAL chunks cleanly.
     */
    public static function resumeRun(string $runId, ?self $pipeline = null): PipelineResult
    {
        $run = PipelineRun::find($runId);
        if ($run === null) {
            throw new InvalidArgumentException("Pipeline run [{$runId}] not found.");
        }

        if ($pipeline === null && $run->name !== null) {
            $pipeline = self::resolveDefinition($run->name);
        }

        if ($pipeline === null) {
            throw new InvalidArgumentException("Cannot resume pipeline [{$runId}]: no definition found for '{$run->name}'. Provide the pipeline instance explicitly or register its definition.");
        }

        // 1. Verify source identity (fingerprint)
        if ($run->source_fingerprint !== null && $pipeline->source->fingerprint() !== $run->source_fingerprint) {
            throw new InvalidArgumentException(
                "Cannot resume pipeline run [{$runId}]: source fingerprint mismatch. The source dataset or configuration has changed."
            );
        }

        // 2. Verify chunk size consistency
        $recordedChunkSize = $run->metadata['chunk_size'] ?? null;
        if ($recordedChunkSize !== null && (int) $recordedChunkSize !== $pipeline->chunkSize) {
            throw new InvalidArgumentException(
                "Cannot resume pipeline run [{$runId}]: chunk size mismatch (original: {$recordedChunkSize}, provided: {$pipeline->chunkSize}). Changing chunk size during resume corrupts chunk sequence boundaries."
            );
        }

        // 3. Verify definition version if registered
        $recordedDefinition = $run->metadata['definition'] ?? null;
        if ($recordedDefinition !== null && $pipeline->definitionKey !== null && $recordedDefinition !== $pipeline->definitionKey) {
            throw new InvalidArgumentException(
                "Cannot resume pipeline run [{$runId}]: definition mismatch (original: '{$recordedDefinition}', provided: '{$pipeline->definitionKey}')."
            );
        }

        $leaseSeconds = max((int) config('data-pipeline.queue.timeout', 60) + 30, (int) config('data-pipeline.queue.lease_seconds', 90));
        if (PipelineChunk::where('pipeline_id', $runId)->where('status', PipelineStatus::Processing->value)
            ->where('started_at', '>', now()->subSeconds($leaseSeconds))->exists()) {
            throw new PipelineBusyException('Cannot resume a pipeline while a worker owns an active chunk lease. Stop its workers and retry after the lease expires.');
        }
        $pipeline->runId = $runId;
        $repository = app(PipelineRepositoryInterface::class);
        $resumeStages = $pipeline->freshStages();

        // Chunks already committed to sink with successful writes
        $committedChunks = PipelineChunk::where('pipeline_id', $runId)
            ->whereIn('status', [PipelineStatus::Completed->value, PipelineStatus::Partial->value])
            ->get();

        $committedSequences = $committedChunks->pluck('sequence')->flip()->all();

        $baselineProcessed = (int) $committedChunks->sum('records_count');
        $baselineSuccessful = (int) $committedChunks->sum('successful_count');
        $baselineFailed = (int) $committedChunks->sum('failed_count');

        $run->update([
            'status' => PipelineStatus::Running->value,
            'processed_records' => $baselineProcessed,
            'successful_records' => $baselineSuccessful,
            'failed_records' => $baselineFailed,
            'updated_at' => now(),
        ]);

        $startTime = microtime(true);
        $totalProcessed = $baselineProcessed;
        $successful = $baselineSuccessful;
        $failed = $baselineFailed;
        $errorBag = new RecordErrorBag;

        $chunkSequence = 1;
        $currentChunk = [];

        foreach ($pipeline->source->records() as $index => $raw) {
            $currentChunk[] = RecordContext::fromRaw($raw, $index);

            if (count($currentChunk) >= $pipeline->chunkSize) {
                $seq = $chunkSequence++;
                if (! isset($committedSequences[$seq])) {
                    $pipeline->processChunk(
                        new RecordChunk($currentChunk, $seq),
                        $totalProcessed,
                        $successful,
                        $failed,
                        $errorBag,
                        $repository,
                        $resumeStages
                    );
                    $totalProcessed += count($currentChunk);
                }
                $currentChunk = [];
            }
        }

        if (! empty($currentChunk)) {
            $seq = $chunkSequence;
            if (! isset($committedSequences[$seq])) {
                $pipeline->processChunk(
                    new RecordChunk($currentChunk, $seq),
                    $totalProcessed,
                    $successful,
                    $failed,
                    $errorBag,
                    $repository,
                    $resumeStages
                );
                $totalProcessed += count($currentChunk);
            }
        }

        $freshRun = PipelineRun::find($runId);
        $remainingErrorsCount = PipelineError::where('pipeline_id', $runId)->count();
        $finalFailed = $freshRun->failed_records ?? $failed;
        if ($remainingErrorsCount === 0 && $finalFailed > 0) {
            $finalFailed = 0;
            $freshRun?->update(['failed_records' => 0]);
        }
        $finalStatus = ($finalFailed > 0 || $remainingErrorsCount > 0) ? PipelineStatus::PartialSuccess->value : PipelineStatus::Completed->value;
        $repository->markRunCompleted($runId, $finalStatus);

        $duration = microtime(true) - $startTime;
        $result = new PipelineResult(
            totalProcessed: $freshRun->processed_records ?? $totalProcessed,
            successful: $freshRun->successful_records ?? $successful,
            failed: $finalFailed,
            errors: $errorBag,
            durationInSeconds: round($duration, 4),
            metadata: ['name' => $pipeline->name, 'run_id' => $runId, 'resumed' => true],
        );

        PipelineCompleted::dispatch($runId, $result);

        return $result;
    }

    /**
     * Surgically retry only the quarantined failed records of a pipeline run.
     * Preserves quarantine payloads, never deleting rows unless they succeed.
     * Streams via chunkById to guarantee strict O(1) memory invariance.
     *
     * @return array{retried: int, recovered: int, remaining_failed: int}
     */
    public static function retryFailedRecords(string $runId, ?self $pipeline = null): array
    {
        $run = PipelineRun::find($runId);
        if ($run === null) {
            throw new InvalidArgumentException("Pipeline run [{$runId}] not found.");
        }

        if ($pipeline === null && $run->name !== null) {
            $pipeline = self::resolveDefinition($run->name);
        }

        if ($pipeline === null) {
            throw new InvalidArgumentException("Cannot retry failed records for pipeline [{$runId}]: no definition found for '{$run->name}'. Provide the pipeline instance explicitly or register its definition.");
        }

        $pipeline->runId = $runId;
        $retried = 0;
        $recovered = 0;
        $repository = app(PipelineRepositoryInterface::class);
        $connection = DB::connection(config('data-pipeline.connection'));
        PipelineError::where('pipeline_id', $runId)->orderBy('id')->chunkById($pipeline->chunkSize, function ($errors) use ($pipeline, $runId, $repository, $connection, &$retried, &$recovered): void {
            $stages = $pipeline->freshStages();
            $stages[] = new RejectRedactedPayloadStage;
            $retried += $errors->count();
            try {
                $count = $connection->transaction(function () use ($errors, $pipeline, $runId, $repository, $stages): int {
                    $locked = PipelineError::whereIn('id', $errors->modelKeys())->lockForUpdate()->get();
                    if ($locked->isEmpty()) {
                        return 0;
                    }
                    $records = [];
                    foreach ($locked as $error) {
                        $records[] = RecordContext::fromRaw($error->raw_payload, $error->record_index, $error->record_identifier);
                    }
                    $recoveredInChunk = 0;
                    app(ChunkExecutor::class)->execute(
                        new RecordChunk($records), $stages, $pipeline->sink, $repository, $runId,
                        transactional: $pipeline->transactional,
                        allowCrossConnectionTransactions: $pipeline->allowCrossConnectionTransactions,
                        redactedAttributes: $pipeline->redactedAttributes,
                        recordCheckpoint: false,
                        afterPersistence: function (ChunkExecutionResult $result) use ($locked, $runId, &$recoveredInChunk): void {
                            $byIndex = $locked->keyBy('record_index');
                            foreach ($result->chunk->records() as $record) {
                                $error = $byIndex->get($record->index());
                                if ($error === null) {
                                    continue;
                                }
                                if ($record->isInvalid() || $record->isDropped()) {
                                    $error->update(['validation_errors' => $record->errors(), 'exception_message' => $record->isDropped() ? 'Record dropped during retry.' : implode('; ', array_merge(...array_values($record->errors()))), 'failed_at' => now()]);
                                } else {
                                    $error->delete();
                                    $recoveredInChunk++;
                                }
                            }
                            PipelineRun::whereKey($runId)->increment('successful_records', $recoveredInChunk);
                        },
                    );

                    return $recoveredInChunk;
                });
                $recovered += $count;
            } catch (Throwable $exception) {
                foreach ($stages as $stage) {
                    if ($stage instanceof DeduplicateStage) {
                        $stage->rollbackClaims();
                    }
                }
                PipelineError::whereIn('id', $errors->modelKeys())->update(['exception_message' => PersistenceError::describe($exception), 'failed_at' => now()]);
            }
        });
        $remaining = PipelineError::where('pipeline_id', $runId)->count();
        $run->update(['failed_records' => $remaining, 'status' => $remaining === 0 ? PipelineStatus::Completed->value : PipelineStatus::PartialSuccess->value, 'updated_at' => now()]);

        return ['retried' => $retried, 'recovered' => $recovered, 'remaining_failed' => $remaining];
    }

    /**
     * @param  array<int, RecordStageInterface|ChunkStageInterface>|null  $stages
     */
    private function processChunk(
        RecordChunk $chunk,
        int $totalProcessed,
        int &$successful,
        int &$failed,
        RecordErrorBag $errorBag,
        ?PipelineRepositoryInterface $repository = null,
        ?array $stages = null
    ): void {
        $started = microtime(true);
        $result = app(ChunkExecutor::class)->execute(
            $chunk, $stages ?? $this->stages, $this->sink, $repository, $this->runId,
            $this->allowPartialSuccess, $this->transactional, $this->allowCrossConnectionTransactions, $this->redactedAttributes,
        );
        $successful += $result->successful;
        $failed += $result->failed();
        foreach ($result->failures as $failure) {
            $errorBag->add($failure);
        }
        try {
            PipelineChunkCompleted::dispatch($this->runId, $chunk->sequence(), $chunk->count(), $result->successful, $result->failed(), round(microtime(true) - $started, 4));
            if ($this->progressCallback !== null) {
                ($this->progressCallback)($totalProcessed, $this->source->count(), $result->successful, $result->failed(), $result->chunk);
            }
        } catch (Throwable $listenerException) {
            report($listenerException);
        }
    }

    /**
     * Return a cloned, fresh execution state of all configured stages.
     *
     * @return array<int, RecordStageInterface|ChunkStageInterface>
     */
    public function freshStages(): array
    {
        return array_map(function ($stage) {
            $cloned = is_object($stage) ? clone $stage : $stage;
            if (is_object($cloned) && method_exists($cloned, 'reset')) {
                $cloned->reset();
            }

            return $cloned;
        }, $this->stages);
    }

    /**
     * @param  array<int, RecordStageInterface|ChunkStageInterface>|null  $stages
     */
    private function executeStagesOnChunk(RecordChunk $chunk, ?array $stages = null): RecordChunk
    {
        return app(ChunkExecutor::class)->processStages($chunk, $stages ?? $this->stages);
    }

    private function resolveSource(mixed $source): SourceInterface
    {
        if ($source instanceof SourceInterface) {
            return $source;
        }

        if (is_string($source) && str_ends_with(strtolower($source), '.csv')) {
            return new CsvSource($source);
        }

        if ($source instanceof Closure) {
            return new LazySource(new LazyCollection($source));
        }

        if ($source instanceof LazyCollection || $source instanceof Generator) {
            return new LazySource($source);
        }

        if ($source instanceof Collection || is_array($source)) {
            return new ArraySource($source);
        }

        if ($source instanceof EloquentBuilder || $source instanceof QueryBuilder || $source instanceof Relation) {
            return new LazySource($source->cursor(), customFingerprint: 'query:'.md5($source->toSql()));
        }

        if ($source instanceof Traversable) {
            return new LazySource($source);
        }

        throw new InvalidArgumentException('Unsupported source type provided to DataPipeline.');
    }
}
