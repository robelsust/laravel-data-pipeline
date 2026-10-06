<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Tests\Unit\Services\DataPipeline;

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Artisan;
use LogicException;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Sinks\ModelSink;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Robel\DataPipeline\Tests\Fixtures\User;
use RuntimeException;

uses(RefreshDatabase::class);

// =========================================================================
// Defect 1: Strict failure can strand valid records
// =========================================================================

it('assigns FAILED status to chunk checkpoints when strict failure prevents sink writes and allows resume to persist stranded valid records', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'],
        ['name' => '',      'email' => 'invalid-email',  'password' => 'secret'],
    ];

    $pipeline = DataPipeline::from($rows)
        ->name('strict-strand-test')
        ->tracked()
        ->chunk(2)
        ->validate(['name' => 'required', 'email' => 'email'])
        ->failIfAny()
        ->insert(User::class);

    $runId = null;

    try {
        $pipeline->run();
    } catch (PipelineValidationException $e) {
        $run = PipelineRun::where('name', 'strict-strand-test')->first();
        expect($run)->not->toBeNull();
        $runId = $run->id;
    }

    expect($runId)->not->toBeNull();

    // Verify ZERO users were inserted into database because strict failure prevented chunk writes
    expect(User::count())->toBe(0);

    // Verify chunk checkpoint received FAILED status (NOT PARTIAL) because zero chunk writes occurred
    $chunk = PipelineChunk::where('pipeline_id', $runId)->where('sequence', 1)->first();
    expect($chunk)->not->toBeNull()
        ->and($chunk->status)->toBe('FAILED')
        ->and($chunk->successful_count)->toBe(0)
        ->and($chunk->failed_count)->toBe(1);

    // Now resume the pipeline allowing partial success
    $resumedPipeline = DataPipeline::from($rows)
        ->name('strict-strand-test')
        ->tracked()
        ->chunk(2)
        ->validate(['name' => 'required', 'email' => 'email'])
        ->allowPartialSuccess()
        ->insert(User::class);

    $result = DataPipeline::resumeRun($runId, $resumedPipeline);

    // Verify the previously stranded valid record was successfully persisted to the database
    expect(User::count())->toBe(1)
        ->and(User::where('email', 'alice@test.com')->exists())->toBeTrue();

    // Verify checkpoint was updated to PARTIAL with accurate counts
    $chunkFresh = PipelineChunk::where('pipeline_id', $runId)->where('sequence', 1)->first();
    expect($chunkFresh->status)->toBe('PARTIAL')
        ->and($chunkFresh->successful_count)->toBe(1)
        ->and($chunkFresh->failed_count)->toBe(1);

    expect($result->successful)->toBe(1)
        ->and($result->failed)->toBe(1);
});

// =========================================================================
// Defect 2: ModelSink reports mixed success incorrectly
// =========================================================================

it('reports mixed success in ModelSink with SinkResult::partial and accurate row indices', function () {
    $record1 = RecordContext::fromRaw([
        'name' => 'Valid User',
        'email' => 'valid@test.com',
        'password' => 'secret123',
    ], 0);

    $record2 = RecordContext::fromRaw([
        'name' => 'Broken User',
        'email' => null, // Column is NOT NULL in database
        'password' => 'secret123',
    ], 1);

    $chunk = new RecordChunk([$record1, $record2], 1);

    $sink = new ModelSink(User::class);
    $result = $sink->persist($chunk);

    // Must return partial result rather than zero success
    expect($result)->toBeInstanceOf(SinkResult::class)
        ->and($result->persistedCount)->toBe(1)
        ->and($result->failedCount)->toBe(1)
        ->and($result->persistedIndices)->toBe([0])
        ->and($result->failedIndices)->toBe([1]);

    // Verify exactly one user exists in the database
    expect(User::count())->toBe(1)
        ->and(User::where('email', 'valid@test.com')->exists())->toBeTrue();
});

it('handles model saving event returning false in ModelSink as a failure', function () {
    User::saving(function (User $user) {
        if ($user->email === 'cancelled@test.com') {
            return false;
        }
    });

    $record1 = RecordContext::fromRaw([
        'name' => 'Alice',
        'email' => 'alice_saved@test.com',
        'password' => 'secret',
    ], 0);

    $record2 = RecordContext::fromRaw([
        'name' => 'Cancelled User',
        'email' => 'cancelled@test.com',
        'password' => 'secret',
    ], 1);

    $chunk = new RecordChunk([$record1, $record2], 1);
    $sink = new ModelSink(User::class);
    $result = $sink->persist($chunk);

    expect($result->persistedCount)->toBe(1)
        ->and($result->failedCount)->toBe(1)
        ->and($result->persistedIndices)->toBe([0])
        ->and($result->failedIndices)->toBe([1])
        ->and(User::where('email', 'alice_saved@test.com')->exists())->toBeTrue()
        ->and(User::where('email', 'cancelled@test.com')->exists())->toBeFalse();

    User::flushEventListeners();
});

// =========================================================================
// Defect 3: Queued deduplication vs Synchronous execution
// =========================================================================

it('deduplicates across chunks consistently in both synchronous and queued execution', function () {
    $rows = [
        ['name' => 'Alice 1', 'email' => 'dup@test.com', 'password' => 'secret'],
        ['name' => 'Alice 2', 'email' => 'dup@test.com', 'password' => 'secret'],
    ];

    // 1. Synchronous run with chunk size 1 (2 chunks)
    $syncResult = DataPipeline::from($rows)
        ->chunk(1)
        ->deduplicate('email')
        ->insert(User::class)
        ->run();

    expect($syncResult->successful)->toBe(1)
        ->and(User::count())->toBe(1);

    User::truncate();

    // 2. Queued run with chunk size 1 (2 chunks)
    config(['queue.default' => 'sync']);

    $runId = DataPipeline::from($rows)
        ->name('queued-dedup-run')
        ->chunk(1)
        ->deduplicate('email')
        ->insert(User::class)
        ->queue();

    // Verify queued execution matches synchronous execution: exactly 1 record persisted
    expect(User::count())->toBe(1);

    $run = PipelineRun::find($runId);
    expect($run->successful_records)->toBe(1)
        ->and($run->status)->toBe('COMPLETED');
});

it('supports explicit chunk-local deduplication when requested', function () {
    $rows = [
        ['name' => 'SharedName', 'email' => 'chunk1@test.com', 'password' => 'secret'],
        ['name' => 'SharedName', 'email' => 'chunk2@test.com', 'password' => 'secret'],
    ];

    config(['queue.default' => 'sync']);

    // When deduplicatePerChunk is explicitly used, separate chunks each retain their copy
    $runId = DataPipeline::from($rows)
        ->name('chunk-local-dedup-run')
        ->chunk(1)
        ->deduplicatePerChunk('name')
        ->insert(User::class)
        ->queue();

    expect(User::count())->toBe(2);

    $run = PipelineRun::find($runId);
    expect($run->successful_records)->toBe(2);
});

// =========================================================================
// Defect 4: Excluded fields respect conditional exclusion in batched rules
// =========================================================================

it('respects exclude_if and does not fail batched database exists rules for excluded fields', function () {
    $rows = [
        // Admin role excludes manager_id, so the non-existent manager_id must NOT fail exists rule
        [
            'name' => 'Admin User',
            'role' => 'admin',
            'manager_id' => '00000000-0000-0000-0000-000000000000',
            'email' => 'admin@test.com',
            'password' => 'secret',
        ],
    ];

    $result = DataPipeline::from($rows)
        ->validate([
            'role' => 'required',
            'manager_id' => 'exclude_if:role,admin|exists:users,id',
            'email' => 'required|email',
        ])
        ->map(fn (array $r) => array_intersect_key($r, array_flip(['name', 'email', 'password'])))
        ->insert(User::class)
        ->run();

    expect($result->successful)->toBe(1)
        ->and($result->failed)->toBe(0)
        ->and(User::where('email', 'admin@test.com')->exists())->toBeTrue();
});

it('fails batched exists rule when field is not excluded', function () {
    $rows = [
        [
            'name' => 'Regular User',
            'role' => 'member',
            'manager_id' => '00000000-0000-0000-0000-000000000000', // Non-existent user id
            'email' => 'member@test.com',
            'password' => 'secret',
        ],
    ];

    $result = DataPipeline::from($rows)
        ->validate([
            'role' => 'required',
            'manager_id' => 'exclude_if:role,admin|exists:users,id',
            'email' => 'required|email',
        ])
        ->map(fn (array $r) => array_intersect_key($r, array_flip(['name', 'email', 'password'])))
        ->insert(User::class)
        ->run();

    expect($result->successful)->toBe(0)
        ->and($result->failed)->toBe(1)
        ->and($result->errors->hasErrors())->toBeTrue();
});

// =========================================================================
// Multi-Connection Transactions Limitation
// =========================================================================

it('rejects multi-connection transactions without explicit acknowledgment', function () {
    config()->set('database.connections.second_db', config('database.connections.sqlite'));

    expect(fn () => DataPipeline::from([['name' => 'Test']])
        ->transactional(true)
        ->insert('second_db.users')
        ->run()
    )->toThrow(LogicException::class, 'cannot guarantee dual-connection atomicity');
});

it('allows multi-connection transactions when explicitly acknowledged via allowCrossConnectionTransactions', function () {
    config()->set('database.connections.second_db', config('database.connections.sqlite'));
    Artisan::call('migrate', ['--database' => 'second_db']);

    $result = DataPipeline::from([
        ['name' => 'Multi-Conn User', 'email' => 'multi@test.com', 'password' => 'secret'],
    ])
        ->transactional(true)
        ->allowCrossConnectionTransactions(true)
        ->insert('second_db.users')
        ->run();

    expect($result->successful)->toBe(1);
});

// =========================================================================
// Defect Remediation: Deduplication Claim Ownership and Rollback/Retry Semantics
// =========================================================================

it('preserves deduplicated records during retry after a transient sink failure rather than dropping them with zero writes', function () {
    $attempt = 0;
    $customSink = new class($attempt) implements SinkInterface
    {
        public function __construct(public int &$attempt) {}

        public function persist(RecordChunk $chunk): SinkResult
        {
            $this->attempt++;
            if ($this->attempt === 1) {
                throw new RuntimeException('Transient database timeout on sink persist');
            }

            foreach ($chunk->valid() as $record) {
                User::create($record->data());
            }

            return SinkResult::success(count($chunk->valid()));
        }
    };

    $rows = [
        ['name' => 'Alice Retry', 'email' => 'alice.retry@test.com', 'password' => 'secret'],
    ];

    $pipeline = DataPipeline::from($rows)
        ->name('dedup-retry-loss-test')
        ->tracked()
        ->chunk(1)
        ->deduplicate('email')
        ->to($customSink);

    // First attempt fails transiently in the sink
    try {
        $pipeline->run();
        test()->fail('Pipeline was expected to throw transient sink exception on attempt 1');
    } catch (RuntimeException $e) {
        expect($e->getMessage())->toBe('Transient database timeout on sink persist');
    }

    // Zero writes occurred on attempt 1
    expect(User::where('email', 'alice.retry@test.com')->count())->toBe(0);

    // Second attempt (retry): ensure record is NOT dropped as duplicate and writes to sink
    $result = $pipeline->run();

    expect($result->successful)->toBe(1)
        ->and($result->failed)->toBe(0)
        ->and(User::where('email', 'alice.retry@test.com')->count())->toBe(1);
});

it('enforces deduplication claim ownership distinguishing retried records from true duplicates', function () {
    $stage = new DeduplicateStage('email', store: 'array', scope: 'test:dedup:');

    $recordA = RecordContext::fromRaw(['id' => 101, 'email' => 'claimed@test.com'], 0);
    $stage->handleRecord($recordA);

    expect($recordA->isDropped())->toBeFalse()
        ->and($stage->hasClaim($recordA))->toBeTrue()
        ->and($stage->getClaimOwner($recordA))->toBe('id:101:idx:0');

    // Simulate retry of the exact same record: same owner token, so it must NOT be dropped
    $retriedRecordA = RecordContext::fromRaw(['id' => 101, 'email' => 'claimed@test.com'], 0);
    $stage->handleRecord($retriedRecordA);

    expect($retriedRecordA->isDropped())->toBeFalse();

    // Now a distinct record with the same email: different owner token, must be dropped as duplicate
    $recordB = RecordContext::fromRaw(['id' => 102, 'email' => 'claimed@test.com'], 1);
    $stage->handleRecord($recordB);

    expect($recordB->isDropped())->toBeTrue();
});

it('rolls back uncommitted deduplication claims on chunk failure to avoid stranding keys', function () {
    $stage = new DeduplicateStage('email', store: 'array', scope: 'test:rollback:');

    $record = RecordContext::fromRaw(['id' => 201, 'email' => 'uncommitted@test.com'], 0);
    $stage->handleRecord($record);

    expect($stage->hasClaim($record))->toBeTrue();

    // Rollback uncommitted claims (e.g. on sink error or transaction abort)
    $stage->rollbackClaims();

    expect($stage->hasClaim($record))->toBeFalse();

    // Next attempt or alternate record can claim the key afresh
    $newRecord = RecordContext::fromRaw(['id' => 202, 'email' => 'uncommitted@test.com'], 0);
    $stage->handleRecord($newRecord);

    expect($newRecord->isDropped())->toBeFalse()
        ->and($stage->hasClaim($newRecord))->toBeTrue();
});

// =========================================================================
// Defect Remediation: Quarantine Error Reconciliation on Checkpoint Replacement (Resume)
// =========================================================================

it('reconciles prior quarantine errors when replacing a checkpoint on resume, deleting obsolete errors when all records recover', function () {
    $rows = [
        ['name' => 'Alice Recover', 'email' => 'alice.recover@test.com', 'password' => 'secret'],
        ['name' => 'Bob',           'email' => 'bob.recover@test.com',   'password' => 'secret'],
    ];

    $pipeline = DataPipeline::from($rows)
        ->name('reconcile-all-recover-test')
        ->tracked()
        ->chunk(2)
        ->validate(['name' => 'required|min:5', 'email' => 'email'])
        ->failIfAny()
        ->insert(User::class);

    $runId = null;
    try {
        $pipeline->run();
    } catch (PipelineValidationException) {
        $run = PipelineRun::where('name', 'reconcile-all-recover-test')->first();
        expect($run)->not->toBeNull();
        $runId = $run->id;
    }

    expect($runId)->not->toBeNull();

    // Zero users inserted due to strict failure
    expect(User::count())->toBe(0);

    // Initial run quarantined 1 error (Bob failed min:5)
    expect(PipelineError::where('pipeline_id', $runId)->count())->toBe(1);

    // Now resume with corrected validation rule so both records recover
    $resumePipeline = DataPipeline::from($rows)
        ->name('reconcile-all-recover-test')
        ->tracked()
        ->chunk(2)
        ->validate(['name' => 'required|min:2', 'email' => 'email'])
        ->failIfAny()
        ->insert(User::class);

    $result = DataPipeline::resumeRun($runId, $resumePipeline);

    // Verify run reports 2 successes and 0 failures
    expect($result->successful)->toBe(2)
        ->and($result->failed)->toBe(0)
        ->and(User::count())->toBe(2);

    // Verify obsolete quarantine entry was reconciled and deleted (0 quarantine entries retained)
    expect(PipelineError::where('pipeline_id', $runId)->count())->toBe(0);
});

it('reconciles prior quarantine errors on resume while preserving entries for records that still fail', function () {
    $rows = [
        ['name' => 'Alice Recovered', 'email' => 'alice.recovered@test.com', 'password' => 'secret'],
        ['name' => 'X',               'email' => 'x.fail@test.com',          'password' => 'secret'],
    ];

    $pipeline = DataPipeline::from($rows)
        ->name('reconcile-partial-recover-test')
        ->tracked()
        ->chunk(2)
        ->validate(['name' => 'required|min:20', 'email' => 'email'])
        ->failIfAny()
        ->insert(User::class);

    $runId = null;
    try {
        $pipeline->run();
    } catch (PipelineValidationException) {
        $run = PipelineRun::where('name', 'reconcile-partial-recover-test')->first();
        expect($run)->not->toBeNull();
        $runId = $run->id;
    }

    expect($runId)->not->toBeNull();
    // Both records failed min:20
    expect(PipelineError::where('pipeline_id', $runId)->count())->toBe(2);

    $originalError2 = PipelineError::where('pipeline_id', $runId)->where('record_index', 1)->first();
    expect($originalError2)->not->toBeNull();

    // Resume with rule relaxed to min:5: Record 0 ('Alice Recovered') succeeds, but Record 1 ('X') STILL invalid
    $resumePipeline = DataPipeline::from($rows)
        ->name('reconcile-partial-recover-test')
        ->tracked()
        ->chunk(2)
        ->validate(['name' => 'required|min:5', 'email' => 'email'])
        ->allowPartialSuccess()
        ->insert(User::class);

    $result = DataPipeline::resumeRun($runId, $resumePipeline);

    expect($result->successful)->toBe(1)
        ->and($result->failed)->toBe(1)
        ->and(User::count())->toBe(1);

    // Verify Record 0's obsolete error was deleted, but Record 1's quarantine entry was preserved!
    $errorsRemaining = PipelineError::where('pipeline_id', $runId)->get();
    expect($errorsRemaining)->toHaveCount(1)
        ->and($errorsRemaining->first()->id)->toBe($originalError2->id)
        ->and($errorsRemaining->first()->record_index)->toBe(1);
});

it('preserves deduplicated records during queue job retry after a transient sink failure', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Queued Dedup Retry Test'
    );

    $attempt = 0;
    $customSink = new class($attempt) implements SinkInterface
    {
        public function __construct(public int &$attempt) {}

        public function persist(RecordChunk $chunk): SinkResult
        {
            $this->attempt++;
            if ($this->attempt === 1) {
                throw new RuntimeException('Transient queue sink timeout');
            }

            foreach ($chunk->valid() as $record) {
                User::create($record->data());
            }

            return SinkResult::success(count($chunk->valid()));
        }
    };

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 0, 'raw' => ['name' => 'Queued Dedup User', 'email' => 'queued.dedup@test.com', 'password' => 'secret']],
        ],
        stages: [
            new DeduplicateStage('email', store: 'array', scope: "pipeline:dedup:{$run->id}:"),
        ],
        sink: $customSink,
    );

    // Attempt 1: throws transient exception
    try {
        $job->handle($repo);
        test()->fail('Expected transient sink exception on attempt 1');
    } catch (RuntimeException $e) {
        expect($e->getMessage())->toBe('Transient queue sink timeout');
    }

    expect(User::where('email', 'queued.dedup@test.com')->count())->toBe(0);

    // Attempt 2: retry redelivery must NOT drop the record as duplicate
    $job->handle($repo);

    expect(User::where('email', 'queued.dedup@test.com')->count())->toBe(1);

    $chunk = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunk->status)->toBe('COMPLETED')
        ->and($chunk->successful_count)->toBe(1)
        ->and($chunk->failed_count)->toBe(0);
});
