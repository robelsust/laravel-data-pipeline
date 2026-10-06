<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(RefreshDatabase::class);

it('executes a full pipeline with mapping, transformation, validation and upsert', function () {
    $rows = [
        ['Full Name' => ' Alice Cooper ', 'Email Address' => 'Alice@Example.com'],
        ['Full Name' => 'Bob Marley',      'Email Address' => 'bob@example.com'],
    ];

    $result = DataPipeline::from($rows)
        ->chunk(10)
        ->mapFields([
            'Full Name' => 'name',
            'Email Address' => 'email',
        ])
        ->transform(function (array $record) {
            $record['name'] = trim($record['name']);
            $record['email'] = strtolower(trim($record['email']));
            $record['password'] = 'hashed-secret';

            return $record;
        })
        ->validate([
            'name' => ['required', 'string'],
            'email' => ['required', 'email'],
        ])
        ->upsert(User::class, uniqueBy: ['email'], update: ['name'])
        ->run();

    expect($result->totalProcessed())->toBe(2)
        ->and($result->successful())->toBe(2)
        ->and($result->failed())->toBe(0)
        ->and($result->isSuccessful())->toBeTrue()
        ->and(User::count())->toBe(2)
        ->and(User::where('email', 'alice@example.com')->first()->name)->toBe('Alice Cooper');
});

it('gracefully handles partial failure by committing valid rows and quarantining invalid rows', function () {
    $rows = [
        ['name' => 'Valid User 1', 'email' => 'valid1@example.com', 'password' => 'secret'],
        ['name' => '',             'email' => 'invalid-email',      'password' => 'secret'], // invalid
        ['name' => 'Valid User 2', 'email' => 'valid2@example.com', 'password' => 'secret'],
    ];

    $result = DataPipeline::from($rows)
        ->chunk(2)
        ->validate([
            'name' => ['required', 'string'],
            'email' => ['required', 'email'],
        ])
        ->allowPartialSuccess()
        ->insert(User::class)
        ->run();

    expect($result->totalProcessed())->toBe(3)
        ->and($result->successful())->toBe(2)
        ->and($result->failed())->toBe(1)
        ->and($result->hasFailures())->toBeTrue()
        ->and(User::count())->toBe(2) // 2 valid users inserted into DB
        ->and(User::where('email', 'valid1@example.com')->exists())->toBeTrue()
        ->and(User::where('email', 'valid2@example.com')->exists())->toBeTrue();

    // Inspect quarantined per-record error
    $failure = $result->errors()->for(1);
    expect($failure)->not->toBeNull()
        ->and($failure->rawInput()['email'])->toBe('invalid-email')
        ->and($failure->errors())->toHaveKeys(['name', 'email']);
});

it('throws PipelineValidationException when failIfAny is enabled', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'valid@test.com'],
        ['name' => '',      'email' => 'invalid-email'],
    ];

    expect(fn () => DataPipeline::from($rows)
        ->validate(['name' => 'required', 'email' => 'email'])
        ->failIfAny()
        ->run()
    )->toThrow(PipelineValidationException::class);
});

it('provides preview of records without persisting to sink', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com'],
        ['name' => 'Bob',   'email' => 'bob@test.com'],
    ];

    $preview = DataPipeline::from($rows)
        ->transform(fn (array $r) => [...$r, 'transformed' => true])
        ->preview(1);

    expect($preview)->toHaveCount(1)
        ->and($preview->first()->get('transformed'))->toBeTrue()
        ->and(User::count())->toBe(0); // No database writes occurred
});

it('executes dry-run simulation across dataset without database writes', function () {
    $rows = [
        ['name' => 'User 1', 'email' => 'u1@test.com'],
        ['name' => '',       'email' => 'bad-email'],
        ['name' => 'User 3', 'email' => 'u3@test.com'],
    ];

    $simulation = DataPipeline::from($rows)
        ->validate([
            'name' => ['required'],
            'email' => ['required', 'email'],
        ])
        ->insert(User::class)
        ->dryRun();

    expect($simulation['total'])->toBe(3)
        ->and($simulation['valid'])->toBe(2)
        ->and($simulation['invalid'])->toBe(1)
        ->and(User::count())->toBe(0); // Zero writes
});

it('deduplicates records inline within the pipeline using deduplicateBy', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'],
        ['name' => 'Bob',   'email' => 'bob@test.com',   'password' => 'secret'],
        ['name' => 'Alice Duplicate', 'email' => 'alice@test.com', 'password' => 'secret'],
    ];

    $result = DataPipeline::from($rows)
        ->deduplicateBy('email')
        ->insert(User::class)
        ->run();

    expect($result->successful())->toBe(2)
        ->and(User::count())->toBe(2)
        ->and(User::pluck('email')->all())->toBe(['alice@test.com', 'bob@test.com']);
});

it('consumes Eloquent Builder as a memory-safe streaming source', function () {
    User::create(['name' => 'Existing 1', 'email' => 'e1@test.com', 'password' => 'secret']);
    User::create(['name' => 'Existing 2', 'email' => 'e2@test.com', 'password' => 'secret']);

    $emails = [];
    DataPipeline::from(User::query()->orderBy('id'))
        ->chunk(1)
        ->persist(function ($chunk) use (&$emails) {
            foreach ($chunk->valid() as $record) {
                $emails[] = $record->get('email');
            }
        })
        ->run();

    expect($emails)->toBe(['e1@test.com', 'e2@test.com']);
});

it('triggers onProgress callback per chunk with execution stats', function () {
    $rows = [
        ['name' => 'User 1', 'email' => 'u1@test.com'],
        ['name' => 'User 2', 'email' => 'u2@test.com'],
        ['name' => 'User 3', 'email' => 'u3@test.com'],
    ];

    $progressReports = [];

    DataPipeline::from($rows)
        ->chunk(2)
        ->onProgress(function (int $processed, ?int $total, int $chunkSuccess, int $chunkFailed) use (&$progressReports) {
            $progressReports[] = [
                'processed' => $processed,
                'total' => $total,
                'chunkSuccess' => $chunkSuccess,
                'chunkFailed' => $chunkFailed,
            ];
        })
        ->run();

    expect($progressReports)->toHaveCount(2)
        ->and($progressReports[0]['processed'])->toBe(2)
        ->and($progressReports[0]['total'])->toBe(3)
        ->and($progressReports[1]['processed'])->toBe(3);
});

it('supports throttling between chunks', function () {
    $rows = [
        ['name' => 'A', 'email' => 'a@test.com'],
        ['name' => 'B', 'email' => 'b@test.com'],
    ];

    $start = microtime(true);

    DataPipeline::from($rows)
        ->chunk(1)
        ->throttle(50) // 50ms throttle between chunk 1 and chunk 2
        ->run();

    $elapsedMs = (microtime(true) - $start) * 1000;
    expect($elapsedMs)->toBeGreaterThanOrEqual(40);
});

it('rolls back chunk modifications when database transaction fails', function () {
    $rows = [
        ['name' => 'Chunk1 User', 'email' => 'c1@test.com', 'password' => 'secret'],
        ['name' => 'Chunk2 Fail', 'email' => 'c2@test.com', 'password' => 'secret'],
    ];

    expect(function () use ($rows) {
        DataPipeline::from($rows)
            ->chunk(1) // 2 separate chunks
            ->transactional(true)
            ->persist(function ($chunk) {
                foreach ($chunk->valid() as $r) {
                    User::create($r->data());
                    if ($r->get('email') === 'c2@test.com') {
                        throw new RuntimeException('Simulated catastrophic database failure during chunk 2');
                    }
                }
            })
            ->run();
    })->toThrow(RuntimeException::class, 'Simulated catastrophic database failure during chunk 2');

    // Chunk 1 committed in its transaction; Chunk 2 rolled back atomically
    expect(User::where('email', 'c1@test.com')->exists())->toBeTrue()
        ->and(User::where('email', 'c2@test.com')->exists())->toBeFalse();
});
