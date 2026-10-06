<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Events\PipelineCompleted;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\RecordFailure;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Event;

uses(RefreshDatabase::class);

it('preserves quarantine records when a custom sink returns SinkResult::failure() during surgical retry', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 2,
        name: 'Retry Quarantine Preservation Test'
    );

    $chunkRecord = $repo->recordChunk(
        runId: $run->id,
        sequence: 1,
        status: 'FAILED',
        recordsCount: 2,
        successfulCount: 0,
        failedCount: 2,
    );

    $repo->quarantineErrors(
        runId: $run->id,
        chunkId: $chunkRecord->id,
        failures: [
            new RecordFailure(
                index: 1,
                identifier: 'usr-1',
                rawInput: ['name' => 'User One', 'email' => 'one@example.com'],
                errors: ['email' => ['Invalid email']],
            ),
            new RecordFailure(
                index: 2,
                identifier: 'usr-2',
                rawInput: ['name' => 'User Two', 'email' => 'two@example.com'],
                errors: ['email' => ['Invalid email']],
            ),
        ]
    );

    expect(PipelineError::where('pipeline_id', $run->id)->count())->toBe(2);

    // Custom sink returning failure
    $failingSink = new class implements SinkInterface
    {
        public function persist(RecordChunk $chunk): SinkResult
        {
            return SinkResult::failure();
        }
    };

    $pipeline = DataPipeline::from([])
        ->chunk(2)
        ->to($failingSink);

    $stats = DataPipeline::retryFailedRecords($run->id, $pipeline);

    expect($stats['retried'])->toBe(2)
        ->and($stats['recovered'])->toBe(0)
        ->and($stats['remaining_failed'])->toBe(2);

    // Verify neither quarantine entry was deleted
    expect(PipelineError::where('pipeline_id', $run->id)->count())->toBe(2);

    // Verify error records were updated with sink failure information
    $errors = PipelineError::where('pipeline_id', $run->id)->get();
    foreach ($errors as $err) {
        expect($err->exception_message)->toContain('Sink persistence failed');
    }
});

it('accurately recovers and deletes only persisted records when sink returns partial result during retry', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 2,
        name: 'Retry Partial Recovery Test'
    );

    $chunkRecord = $repo->recordChunk(
        runId: $run->id,
        sequence: 1,
        status: 'FAILED',
        recordsCount: 2,
        successfulCount: 0,
        failedCount: 2,
    );

    $repo->quarantineErrors(
        runId: $run->id,
        chunkId: $chunkRecord->id,
        failures: [
            new RecordFailure(1, '1', ['name' => 'A'], ['name' => ['error']]),
            new RecordFailure(2, '2', ['name' => 'B'], ['name' => ['error']]),
        ]
    );

    // Partial sink: record 2 persists, record 1 fails
    $partialSink = new class implements SinkInterface
    {
        public function persist(RecordChunk $chunk): SinkResult
        {
            return SinkResult::partial(
                persisted: 1,
                failed: 1,
                metadata: ['error' => 'Disk full for record 1'],
                persistedIndices: [2],
                failedIndices: [1],
            );
        }
    };

    $pipeline = DataPipeline::from([])
        ->chunk(2)
        ->to($partialSink);

    $stats = DataPipeline::retryFailedRecords($run->id, $pipeline);

    expect($stats['retried'])->toBe(2)
        ->and($stats['recovered'])->toBe(1)
        ->and($stats['remaining_failed'])->toBe(1);

    // Record 1 failed -> MUST still exist in quarantine
    expect(PipelineError::where('pipeline_id', $run->id)->where('record_index', 1)->exists())->toBeTrue();

    // Record 2 persisted -> MUST be deleted from quarantine
    expect(PipelineError::where('pipeline_id', $run->id)->where('record_index', 2)->exists())->toBeFalse();
});

it('preserves all quarantine entries when sink returns partial counts without row-level attribution', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 2,
        name: 'Ambiguous Partial Recovery Test'
    );

    $chunkRecord = $repo->recordChunk(
        runId: $run->id,
        sequence: 1,
        status: 'FAILED',
        recordsCount: 2,
        successfulCount: 0,
        failedCount: 2,
    );

    $repo->quarantineErrors(
        runId: $run->id,
        chunkId: $chunkRecord->id,
        failures: [
            new RecordFailure(1, '1', ['name' => 'A'], ['name' => ['error']]),
            new RecordFailure(2, '2', ['name' => 'B'], ['name' => ['error']]),
        ]
    );

    // Partial sink with naked counts: 1 succeeded, 1 failed, but zero row-level attribution
    $ambiguousSink = new class implements SinkInterface
    {
        public function persist(RecordChunk $chunk): SinkResult
        {
            return SinkResult::partial(1, 1, ['error' => 'One record failed']);
        }
    };

    $pipeline = DataPipeline::from([])
        ->chunk(2)
        ->to($ambiguousSink);

    $stats = DataPipeline::retryFailedRecords($run->id, $pipeline);

    // Zero records are deleted by arbitrary offset guessing; both remain quarantined
    expect($stats['retried'])->toBe(2)
        ->and($stats['recovered'])->toBe(0)
        ->and($stats['remaining_failed'])->toBe(2);

    expect(PipelineError::where('pipeline_id', $run->id)->count())->toBe(2);
});

it('fails explicitly with RuntimeException when queue job cannot resolve pipeline definition', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Unresolved Definition Run'
    );

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Unresolved', 'email' => 'unresolved@test.com']],
        ],
        pipelineDefinition: 'non_existent_registered_pipeline_definition',
    );

    expect(fn () => $job->handle($repo))
        ->toThrow(
            RuntimeException::class,
            "Cannot process pipeline chunk #1 for run [{$run->id}]: Pipeline definition 'non_existent_registered_pipeline_definition' could not be resolved."
        );

    // Ensure chunk was marked FAILED rather than COMPLETED
    $chunk = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunk)->not->toBeNull()
        ->and($chunk->status)->toBe('FAILED');

    // Ensure zero records were falsely reported as successful
    $updatedRun = PipelineRun::find($run->id);
    expect($updatedRun->successful_records)->toBe(0);
});

it('emits PipelineCompleted exactly once at the end when processing 51 chunks on sync queue', function () {
    $records = [];
    for ($i = 1; $i <= 51; $i++) {
        $records[] = [
            'name' => "User {$i}",
            'email' => "user{$i}@syncbatch.com",
            'password' => 'secret123',
        ];
    }

    $completedEvents = [];
    Event::listen(PipelineCompleted::class, function (PipelineCompleted $event) use (&$completedEvents) {
        $completedEvents[] = $event;
    });

    config(['queue.default' => 'sync']);

    $runId = DataPipeline::from($records)
        ->name('51 Chunks Sync Run')
        ->chunk(1)
        ->insert(User::class)
        ->queue();

    // Verify all 51 users were inserted
    expect(User::where('email', 'like', '%@syncbatch.com')->count())->toBe(51);

    // Verify PipelineCompleted was emitted exactly ONCE (not twice, and not prematurely at 50)
    expect($completedEvents)->toHaveCount(1)
        ->and($completedEvents[0]->runId)->toBe($runId)
        ->and($completedEvents[0]->result->successful)->toBe(51)
        ->and($completedEvents[0]->result->totalProcessed)->toBe(51);

    $run = PipelineRun::find($runId);
    expect($run->status)->toBe('COMPLETED')
        ->and($run->processed_records)->toBe(51)
        ->and($run->successful_records)->toBe(51)
        ->and($run->failed_records)->toBe(0);

    // Verify 51 chunk checkpoint records exist and all are COMPLETED
    $chunks = PipelineChunk::where('pipeline_id', $runId)->get();
    expect($chunks)->toHaveCount(51);
    foreach ($chunks as $chunk) {
        expect($chunk->status)->toBe('COMPLETED');
    }
});

it('protects against concurrent duplicate delivery of the same chunk across multiple workers', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Concurrent Delivery Run'
    );

    $job1 = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Concurrent User', 'email' => 'concurrent@test.com', 'password' => 'secret']],
        ],
        sink: new InsertSink(User::class),
    );

    $job2 = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Concurrent User', 'email' => 'concurrent@test.com', 'password' => 'secret']],
        ],
        sink: new InsertSink(User::class),
    );

    // Worker 1 acquires chunk lease and begins processing (status = PROCESSING)
    PipelineChunk::create([
        'pipeline_id' => $run->id,
        'sequence' => 1,
        'status' => 'PROCESSING',
        'records_count' => 1,
        'started_at' => now(),
    ]);

    // Worker 2 attempts to process concurrently while Worker 1 is in PROCESSING status
    $job2->handle($repo);

    // Assert Worker 2 bailed out: 0 records inserted, run counters remain 0
    expect(User::where('email', 'concurrent@test.com')->count())->toBe(0);
    $runFresh = PipelineRun::find($run->id);
    expect($runFresh->processed_records)->toBe(0)
        ->and($runFresh->successful_records)->toBe(0);

    // Reset status so Worker 1 can proceed
    PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->delete();
    $job1->handle($repo);

    expect(User::where('email', 'concurrent@test.com')->count())->toBe(1);
    $runFreshAfter = PipelineRun::find($run->id);
    expect($runFreshAfter->processed_records)->toBe(1)
        ->and($runFreshAfter->successful_records)->toBe(1);

    // Worker 2 redelivers after completion: bails out immediately
    $job2->handle($repo);
    expect(User::where('email', 'concurrent@test.com')->count())->toBe(1);
    $runFreshFinal = PipelineRun::find($run->id);
    expect($runFreshFinal->processed_records)->toBe(1)
        ->and($runFreshFinal->successful_records)->toBe(1);
});

it('bounds memory in DeduplicateStage guaranteeing strict O(1) memory with bounded capacity', function () {
    $maxKeys = 100;
    $stage = new DeduplicateStage('id', action: 'drop', maxKeys: $maxKeys);

    // Stream 1,000 unique records through stage
    for ($i = 1; $i <= 1000; $i++) {
        $record = RecordContext::fromRaw(['id' => "key_{$i}"], $i);
        $stage->handleRecord($record);

        // At no point may seen count exceed maxKeys
        expect($stage->seenCount())->toBeLessThanOrEqual($maxKeys);
    }

    // Final seen count is bounded by maxKeys
    expect($stage->seenCount())->toBeLessThanOrEqual($maxKeys)
        ->and($stage->seenCount())->toBeGreaterThan(0);
});

it('streams quarantine retries via chunkById to guarantee O(1) memory during multi-chunk retries', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 10,
        name: 'Streaming Quarantine Retry'
    );

    $chunkRecord = $repo->recordChunk(
        runId: $run->id,
        sequence: 1,
        status: 'FAILED',
        recordsCount: 10,
        successfulCount: 0,
        failedCount: 10,
    );

    $failures = [];
    for ($i = 1; $i <= 10; $i++) {
        $failures[] = new RecordFailure(
            index: $i,
            identifier: "usr-{$i}",
            rawInput: ['name' => "User {$i}", 'email' => "stream{$i}@test.com", 'password' => 'secret'],
            errors: ['name' => ['Initially failed']],
        );
    }
    $repo->quarantineErrors($run->id, $chunkRecord->id, $failures);

    expect(PipelineError::where('pipeline_id', $run->id)->count())->toBe(10);

    // Pipeline with small chunk size (3) to test multi-chunk chunkById iteration
    $pipeline = DataPipeline::from([])
        ->chunk(3)
        ->insert(User::class);

    $stats = DataPipeline::retryFailedRecords($run->id, $pipeline);

    expect($stats['retried'])->toBe(10)
        ->and($stats['recovered'])->toBe(10)
        ->and($stats['remaining_failed'])->toBe(0);

    // Verify all 10 users were inserted into database
    expect(User::where('email', 'like', 'stream%@test.com')->count())->toBe(10);

    // Quarantine entries completely cleared
    expect(PipelineError::where('pipeline_id', $run->id)->count())->toBe(0);

    $updatedRun = PipelineRun::find($run->id);
    expect($updatedRun->status)->toBe('COMPLETED')
        ->and($updatedRun->successful_records)->toBe(10)
        ->and($updatedRun->failed_records)->toBe(0);
});

it('allows failed jobs to retry normally after a thrown sink exception rather than skipping redelivery', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Failed Job Retry Test'
    );

    $shouldFail = true;

    $flakySink = new class($shouldFail) implements SinkInterface
    {
        public function __construct(public bool &$shouldFail) {}

        public function persist(RecordChunk $chunk): SinkResult
        {
            if ($this->shouldFail) {
                throw new RuntimeException('Transient downstream failure');
            }

            foreach ($chunk->records() as $rec) {
                User::create([
                    'name' => $rec->data()['name'],
                    'email' => $rec->data()['email'],
                    'password' => 'secret',
                ]);
            }

            return SinkResult::success(count($chunk->records()));
        }
    };

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Retryable User', 'email' => 'retryable@test.com']],
        ],
        sink: $flakySink,
    );

    // Attempt 1: fails with thrown exception
    expect(fn () => $job->handle($repo))->toThrow(RuntimeException::class, 'Transient downstream failure');

    // Verify chunk status was set to FAILED (not stuck in PROCESSING)
    $chunk = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunk)->not->toBeNull()
        ->and($chunk->status)->toBe('FAILED');

    // Attempt 2: transient error resolved, redelivery must NOT be skipped
    $shouldFail = false;
    $job->handle($repo);

    // Verify user was inserted
    expect(User::where('email', 'retryable@test.com')->exists())->toBeTrue();

    // Verify chunk status is now COMPLETED
    $chunkAfter = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunkAfter->status)->toBe('COMPLETED')
        ->and($chunkAfter->successful_count)->toBe(1);
});

it('prevents retry attempt from reclaiming an active fresh PROCESSING lease held by another worker', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Active Lease Overlap Prevention Test'
    );

    // Active Worker 1 holds lease (status = PROCESSING, started 5 seconds ago)
    $chunk = PipelineChunk::create([
        'pipeline_id' => $run->id,
        'sequence' => 1,
        'status' => 'PROCESSING',
        'records_count' => 1,
        'started_at' => now()->subSeconds(5),
    ]);

    $jobAttempt2 = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Concurrent User', 'email' => 'active_lease@test.com', 'password' => 'secret']],
        ],
        sink: new InsertSink(User::class),
    );

    // Simulate queue worker attempt 2 while Worker 1 is still actively PROCESSING
    $mockQueueJob = Mockery::mock(Job::class);
    $mockQueueJob->shouldReceive('attempts')->andReturn(2);
    $jobAttempt2->setJob($mockQueueJob);

    // Attempt 2 must NOT take the active lease
    $jobAttempt2->handle($repo);

    // Assert Attempt 2 bailed out: user was not inserted
    expect(User::where('email', 'active_lease@test.com')->count())->toBe(0);

    // Chunk started_at was not altered
    $freshChunk = PipelineChunk::find($chunk->id);
    expect($freshChunk->status)->toBe('PROCESSING')
        ->and($freshChunk->started_at->timestamp)->toBe($chunk->started_at->timestamp);
});

it('prevents duplicate processing when completion event listener throws an exception after persistence commits', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Listener Failure Duplicate Prevention Test'
    );

    Event::listen(PipelineChunkCompleted::class, function () {
        throw new RuntimeException('Telemetry listener failure after persistence');
    });

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Committed User', 'email' => 'listener_err@test.com', 'password' => 'secret']],
        ],
        sink: new InsertSink(User::class),
    );

    // Run job: persistence commits, listener throws and is reported without marking chunk FAILED
    $job->handle($repo);

    expect(User::where('email', 'listener_err@test.com')->count())->toBe(1);

    // Chunk status MUST remain COMPLETED (not marked FAILED)
    $chunk = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunk->status)->toBe('COMPLETED');

    // Run progress incremented exactly once
    $freshRun = PipelineRun::find($run->id);
    expect($freshRun->successful_records)->toBe(1)
        ->and($freshRun->processed_records)->toBe(1);

    // Redelivery: queue worker retries job
    $job->handle($repo);

    // Must NOT execute sink again or double-count records
    expect(User::where('email', 'listener_err@test.com')->count())->toBe(1);
    $freshRunAfter = PipelineRun::find($run->id);
    expect($freshRunAfter->successful_records)->toBe(1)
        ->and($freshRunAfter->processed_records)->toBe(1);
});
