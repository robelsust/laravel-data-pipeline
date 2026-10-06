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
use Robel\DataPipeline\Events\PipelineRecordFailed;
use Robel\DataPipeline\Events\PipelineStarted;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
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
use Robel\DataPipeline\Stages\ResolveRelationStage;
use Robel\DataPipeline\Stages\TransformStage;
use Robel\DataPipeline\Stages\ValidateStage;
use Throwable;
use Traversable;

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
            if (is_string($def) && class_exists($def)) {
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
                    status: $failed > 0 ? 'PARTIAL_SUCCESS' : 'COMPLETED'
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
                $repository->markRunFailed($this->runId, $e->getMessage());
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

                $status = $hasFailures ? 'PARTIAL_SUCCESS' : 'COMPLETED';
                $repo->markRunCompleted($runId, $status);

                PipelineCompleted::dispatch($runId, new PipelineResult(
                    totalProcessed: $runModel?->processed_records ?? 0,
                    successful: $runModel?->successful_records ?? 0,
                    failed: $runModel?->failed_records ?? 0,
                    errors: new RecordErrorBag,
                    durationInSeconds: 0.0,
                    metadata: ['batch_id' => $batch->id],
                ));
            })
            ->catch(function (Batch $batch, Throwable $e) use ($runId) {
                $repo = app(PipelineRepositoryInterface::class);
                $repo->markRunFailed($runId, $e->getMessage());

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

        $pipeline->runId = $runId;
        $repository = app(PipelineRepositoryInterface::class);
        $resumeStages = $pipeline->freshStages();

        // Chunks already committed to sink with successful writes
        $committedChunks = PipelineChunk::where('pipeline_id', $runId)
            ->whereIn('status', ['COMPLETED', 'PARTIAL'])
            ->where('successful_count', '>', 0)
            ->get();

        $committedSequences = $committedChunks->pluck('sequence')->flip()->all();

        $baselineProcessed = (int) $committedChunks->sum('records_count');
        $baselineSuccessful = (int) $committedChunks->sum('successful_count');
        $baselineFailed = (int) $committedChunks->sum('failed_count');

        $run->update([
            'status' => 'RUNNING',
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
        $finalFailed = $freshRun?->failed_records ?? $failed;
        if ($remainingErrorsCount === 0 && $finalFailed > 0) {
            $finalFailed = 0;
            $freshRun?->update(['failed_records' => 0]);
        }
        $finalStatus = ($finalFailed > 0 || $remainingErrorsCount > 0) ? 'PARTIAL_SUCCESS' : 'COMPLETED';
        $repository->markRunCompleted($runId, $finalStatus);

        $duration = microtime(true) - $startTime;
        $result = new PipelineResult(
            totalProcessed: $freshRun?->processed_records ?? $totalProcessed,
            successful: $freshRun?->successful_records ?? $successful,
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
        $hasErrors = PipelineError::where('pipeline_id', $runId)->exists();

        if (! $hasErrors) {
            return [
                'retried' => 0,
                'recovered' => 0,
                'remaining_failed' => 0,
            ];
        }

        $totalRetried = 0;
        $recoveredCount = 0;
        $stillFailedCount = 0;

        PipelineError::where('pipeline_id', $runId)
            ->orderBy('id')
            ->chunkById($pipeline->chunkSize, function ($errorBatch) use (
                $pipeline,
                &$totalRetried,
                &$recoveredCount,
                &$stillFailedCount
            ) {
                $totalRetried += $errorBatch->count();
                $contexts = [];
                $errorModelMap = [];

                foreach ($errorBatch as $errorRecord) {
                    $ctx = RecordContext::fromRaw($errorRecord->raw_payload, $errorRecord->record_index);
                    if ($errorRecord->record_identifier !== null) {
                        $ctx->setIdentifier($errorRecord->record_identifier);
                    }
                    $contexts[] = $ctx;
                    $errorModelMap[$errorRecord->record_index] = $errorRecord;
                }

                $chunk = new RecordChunk($contexts, 1);
                $retryStages = $pipeline->freshStages();
                $processed = $pipeline->executeStagesOnChunk($chunk, $retryStages);

                $validForSink = [];

                foreach ($processed->records() as $rec) {
                    $errorRecord = $errorModelMap[$rec->index()] ?? null;
                    if (! $errorRecord) {
                        continue;
                    }

                    if ($rec->isInvalid() || $rec->isDropped()) {
                        $errorRecord->update([
                            'validation_errors' => $rec->errors(),
                            'failed_at' => now(),
                        ]);
                        $stillFailedCount++;
                    } else {
                        $validForSink[] = $rec;
                    }
                }

                if (! empty($validForSink)) {
                    $recoveredChunk = new RecordChunk($validForSink, 1);

                    if ($pipeline->sink !== null) {
                        try {
                            $sinkResult = $pipeline->sink->persist($recoveredChunk);
                        } catch (Throwable $e) {
                            $sinkResult = SinkResult::failure(
                                count($recoveredChunk->records()),
                                ['exception' => $e->getMessage()]
                            );
                        }

                        // Inspect sinkResult: if sink persisted zero records, preserve quarantine
                        if ($sinkResult->persistedCount === 0) {
                            foreach ($retryStages as $stage) {
                                if ($stage instanceof DeduplicateStage) {
                                    $stage->rollbackClaims();
                                }
                            }

                            $errorMessage = $sinkResult->metadata['exception']
                                ?? ($sinkResult->metadata['error'] ?? 'Sink persistence failed.');

                            foreach ($recoveredChunk->records() as $rec) {
                                $errorRecord = $errorModelMap[$rec->index()] ?? null;
                                if ($errorRecord) {
                                    $rec->addError('_sink', $errorMessage);
                                    $errorRecord->update([
                                        'validation_errors' => $rec->errors(),
                                        'exception_message' => $errorMessage,
                                        'failed_at' => now(),
                                    ]);
                                    $stillFailedCount++;
                                }
                            }
                        } else {
                            $totalChunkCount = count($recoveredChunk->records());
                            $allPersisted = ($sinkResult->persistedCount >= $totalChunkCount && $sinkResult->failedCount === 0);

                            // Extract positive and negative row-level attributions
                            $persistedIndices = array_flip(
                                ! empty($sinkResult->persistedIndices)
                                    ? $sinkResult->persistedIndices
                                    : ($sinkResult->metadata['persisted_indices'] ?? [])
                            );
                            $failedIndices = array_flip(
                                ! empty($sinkResult->failedIndices)
                                    ? $sinkResult->failedIndices
                                    : ($sinkResult->metadata['failed_indices'] ?? [])
                            );

                            $hasExplicitAttribution = ! empty($persistedIndices) || ! empty($failedIndices);

                            foreach ($recoveredChunk->records() as $rec) {
                                $errorRecord = $errorModelMap[$rec->index()] ?? null;
                                if (! $errorRecord) {
                                    continue;
                                }

                                $index = $rec->index();
                                $identifier = $rec->identifier();

                                // 1. Determine if this record specifically failed
                                $isExplicitlyFailed = $rec->isInvalid()
                                    || isset($failedIndices[$index])
                                    || ($identifier !== null && isset($failedIndices[$identifier]));

                                // 2. Determine if this record specifically persisted
                                $isExplicitlyPersisted = isset($persistedIndices[$index])
                                    || ($identifier !== null && isset($persistedIndices[$identifier]));

                                if ($isExplicitlyFailed) {
                                    $errorRecord->update([
                                        'validation_errors' => $rec->errors(),
                                        'exception_message' => $sinkResult->metadata['error'] ?? 'Sink persistence failed for this record.',
                                        'failed_at' => now(),
                                    ]);
                                    $stillFailedCount++;
                                } elseif ($isExplicitlyPersisted || ($allPersisted && ! $rec->isInvalid())) {
                                    // Confirmed persisted: delete from quarantine
                                    $errorRecord->delete();
                                    $recoveredCount++;
                                } elseif ($hasExplicitAttribution && ! $isExplicitlyPersisted) {
                                    // Other records were explicitly persisted, but this one was not
                                    $errorRecord->update([
                                        'exception_message' => 'Record not persisted by sink.',
                                        'failed_at' => now(),
                                    ]);
                                    $stillFailedCount++;
                                } else {
                                    // Partial result without row-level attribution: preserve in quarantine to prevent data loss
                                    $errorRecord->update([
                                        'exception_message' => "Ambiguous partial sink persistence ({$sinkResult->persistedCount}/{$totalChunkCount} persisted); retained in quarantine to protect payload.",
                                        'failed_at' => now(),
                                    ]);
                                    $stillFailedCount++;
                                }
                            }

                            if ($sinkResult->failedCount > 0) {
                                $failedList = ! empty($sinkResult->failedIndices)
                                    ? $sinkResult->failedIndices
                                    : ($sinkResult->metadata['failed_indices'] ?? []);
                                foreach ($retryStages as $stage) {
                                    if ($stage instanceof DeduplicateStage) {
                                        $stage->rollbackClaims($failedList);
                                        $stage->commitClaims();
                                    }
                                }
                            } else {
                                foreach ($retryStages as $stage) {
                                    if ($stage instanceof DeduplicateStage) {
                                        $stage->commitClaims();
                                    }
                                }
                            }
                        }
                    } else {
                        foreach ($validForSink as $rec) {
                            $errorRecord = $errorModelMap[$rec->index()] ?? null;
                            $errorRecord?->delete();
                            $recoveredCount++;
                        }
                        foreach ($retryStages as $stage) {
                            if ($stage instanceof DeduplicateStage) {
                                $stage->commitClaims();
                            }
                        }
                    }
                }
            });

        $remainingFailed = PipelineError::where('pipeline_id', $runId)->count();
        $run->update([
            'successful_records' => $run->successful_records + $recoveredCount,
            'failed_records' => $remainingFailed,
            'status' => $remainingFailed === 0 ? 'COMPLETED' : 'PARTIAL_SUCCESS',
            'updated_at' => now(),
        ]);

        return [
            'retried' => $totalRetried,
            'recovered' => $recoveredCount,
            'remaining_failed' => $remainingFailed,
        ];
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
        $chunkStartTime = microtime(true);

        $execute = function () use (
            $chunk,
            $stages,
            $totalProcessed,
            &$successful,
            &$failed,
            $errorBag,
            $repository,
            $chunkStartTime
        ) {
            $processedChunk = $this->executeStagesOnChunk($chunk, $stages);
            $chunkFailures = [];

            // Record invalid rows in error bag and dispatch PipelineRecordFailed
            foreach ($processedChunk->invalid() as $invalidRecord) {
                $failed++;
                $failure = new RecordFailure(
                    index: $invalidRecord->index(),
                    identifier: $invalidRecord->identifier(),
                    rawInput: $invalidRecord->raw(),
                    errors: $invalidRecord->errors(),
                );
                $errorBag->add($failure);
                $chunkFailures[] = $failure;

                PipelineRecordFailed::dispatch($this->runId, $failure);
            }

            $chunkSuccessful = 0;
            $chunkFailed = count($chunkFailures);

            // Strict failure: failIfAny prevents sink writes for chunks containing failures
            if (! $this->allowPartialSuccess && $chunkFailed > 0) {
                // Do not persist this chunk to sink
                foreach ($stages ?? $this->stages as $stage) {
                    if ($stage instanceof DeduplicateStage) {
                        $stage->rollbackClaims();
                    }
                }
            } elseif ($this->sink !== null) {
                try {
                    $sinkResult = $this->sink->persist($processedChunk);
                } catch (Throwable $sinkEx) {
                    foreach ($stages ?? $this->stages as $stage) {
                        if ($stage instanceof DeduplicateStage) {
                            $stage->rollbackClaims();
                        }
                    }
                    throw $sinkEx;
                }

                $successful += $sinkResult->persistedCount;
                $chunkSuccessful += $sinkResult->persistedCount;

                if ($sinkResult->persistedCount === 0 && $sinkResult->failedCount > 0) {
                    foreach ($stages ?? $this->stages as $stage) {
                        if ($stage instanceof DeduplicateStage) {
                            $stage->rollbackClaims();
                        }
                    }
                } elseif ($sinkResult->failedCount > 0) {
                    $failedIndices = array_map(fn ($f) => $f->index(), $chunkFailures);
                    foreach ($stages ?? $this->stages as $stage) {
                        if ($stage instanceof DeduplicateStage) {
                            $stage->rollbackClaims($failedIndices);
                            $stage->commitClaims();
                        }
                    }
                } else {
                    foreach ($stages ?? $this->stages as $stage) {
                        if ($stage instanceof DeduplicateStage) {
                            $stage->commitClaims();
                        }
                    }
                }

                // If the sink failed some rows, capture them across ALL chunk records
                if ($sinkResult->failedCount > 0) {
                    $failed += $sinkResult->failedCount;
                    $chunkFailed += $sinkResult->failedCount;
                    $alreadyFailedIndices = array_flip(array_map(fn ($f) => $f->index(), $chunkFailures));

                    foreach ($processedChunk->records() as $r) {
                        if ($r->isInvalid() && ! isset($alreadyFailedIndices[$r->index()])) {
                            $failure = new RecordFailure(
                                index: $r->index(),
                                identifier: $r->identifier(),
                                rawInput: $r->raw(),
                                errors: $r->errors(),
                            );
                            $errorBag->add($failure);
                            $chunkFailures[] = $failure;
                            $alreadyFailedIndices[$r->index()] = true;

                            PipelineRecordFailed::dispatch($this->runId, $failure);
                        }
                    }

                    if (count($chunkFailures) < $chunkFailed) {
                        foreach ($processedChunk->records() as $r) {
                            if (! isset($alreadyFailedIndices[$r->index()])) {
                                $r->addError('_sink', $sinkResult->metadata['exception'] ?? ($sinkResult->metadata['error'] ?? 'Sink persistence failed.'));
                                $failure = new RecordFailure(
                                    index: $r->index(),
                                    identifier: $r->identifier(),
                                    rawInput: $r->raw(),
                                    errors: $r->errors(),
                                );
                                $errorBag->add($failure);
                                $chunkFailures[] = $failure;
                                $alreadyFailedIndices[$r->index()] = true;

                                PipelineRecordFailed::dispatch($this->runId, $failure);
                            }
                        }
                    }
                }
            } else {
                $validCount = count($processedChunk->valid());
                $successful += $validCount;
                $chunkSuccessful += $validCount;

                foreach ($stages ?? $this->stages as $stage) {
                    if ($stage instanceof DeduplicateStage) {
                        $stage->commitClaims();
                    }
                }
            }

            // Persist chunk checkpoint and errors to database repository if tracking enabled
            if ($repository !== null && $this->runId !== null) {
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
                    sequence: $chunk->sequence(),
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
            }

            $chunkDuration = round(microtime(true) - $chunkStartTime, 4);

            PipelineChunkCompleted::dispatch(
                $this->runId,
                $chunk->sequence(),
                $chunk->count(),
                $chunkSuccessful,
                $chunkFailed,
                $chunkDuration,
            );

            if ($this->progressCallback !== null) {
                ($this->progressCallback)(
                    $totalProcessed,
                    $this->source->count(),
                    $chunkSuccessful,
                    $chunkFailed,
                    $chunk,
                );
            }
        };

        $destConnection = ($this->sink !== null && method_exists($this->sink, 'getConnection'))
            ? $this->sink->getConnection()
            : null;
        $trackingConnection = config('database.default');

        try {
            if ($this->transactional && $this->sink !== null) {
                if ($destConnection !== null && $destConnection !== $trackingConnection) {
                    if (! $this->allowCrossConnectionTransactions) {
                        throw new \LogicException(
                            "Transactional pipeline execution cannot guarantee dual-connection atomicity across destination connection '{$destConnection}' and tracking connection '{$trackingConnection}'. Either configure both to use the same connection, or call ->allowCrossConnectionTransactions() to explicitly proceed with nested commits."
                        );
                    }

                    DB::connection($trackingConnection)->transaction(function () use ($destConnection, $execute) {
                        DB::connection($destConnection)->transaction(function () use ($execute) {
                            $execute();
                        });
                    });
                } else {
                    $conn = $destConnection ?? $trackingConnection;
                    DB::connection($conn)->transaction($execute);
                }
            } else {
                $execute();
            }
        } catch (Throwable $e) {
            foreach ($stages ?? $this->stages as $stage) {
                if ($stage instanceof DeduplicateStage) {
                    $stage->rollbackClaims();
                }
            }

            throw $e;
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
        $stages = $stages ?? $this->stages;

        // Execute stages preserving exact fluent registration order
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

        return $chunk;
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
