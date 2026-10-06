<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

it('lists pipeline runs via pipeline:list', function () {
    PipelineRun::create([
        'name' => 'Daily Catalog Sync',
        'status' => 'COMPLETED',
        'source_type' => 'ArraySource',
        'total_records' => 5000,
        'processed_records' => 5000,
        'successful_records' => 5000,
        'failed_records' => 0,
        'started_at' => now(),
        'completed_at' => now(),
    ]);

    $this->artisan('pipeline:list')
        ->expectsOutputToContain('Daily Catalog Sync')
        ->assertSuccessful();
});

it('filters pipeline runs by status via pipeline:list --status', function () {
    PipelineRun::create([
        'name' => 'Completed Ingest',
        'status' => 'COMPLETED',
        'source_type' => 'ArraySource',
        'total_records' => 10,
        'started_at' => now(),
    ]);

    PipelineRun::create([
        'name' => 'Failed Ingest',
        'status' => 'FAILED',
        'source_type' => 'ArraySource',
        'total_records' => 10,
        'started_at' => now(),
    ]);

    $this->artisan('pipeline:list --status=FAILED')
        ->expectsOutputToContain('Failed Ingest')
        ->doesntExpectOutputToContain('Completed Ingest')
        ->assertSuccessful();
});

it('inspects a pipeline run via pipeline:show', function () {
    $run = PipelineRun::create([
        'name' => 'E-commerce Ingest',
        'status' => 'PARTIAL_SUCCESS',
        'source_type' => 'CsvSource',
        'total_records' => 100,
        'processed_records' => 100,
        'successful_records' => 98,
        'failed_records' => 2,
        'started_at' => now(),
        'completed_at' => now(),
    ]);

    $chunk = PipelineChunk::create([
        'pipeline_id' => $run->id,
        'sequence' => 1,
        'status' => 'PARTIAL',
        'records_count' => 100,
        'successful_count' => 98,
        'failed_count' => 2,
    ]);

    PipelineError::create([
        'pipeline_id' => $run->id,
        'chunk_id' => $chunk->id,
        'record_index' => 42,
        'record_identifier' => 'sku-42',
        'validation_errors' => ['email' => ['Invalid email']],
    ]);

    $this->artisan('pipeline:show', ['id' => $run->id])
        ->expectsOutputToContain('Pipeline Run: E-commerce Ingest')
        ->expectsOutputToContain('Succeeded: 98')
        ->expectsOutputToContain('Failed:    2')
        ->assertSuccessful();
});

it('resumes a paused or failed pipeline via pipeline:resume', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'],
        ['name' => 'Bob',   'email' => 'bob@test.com',   'password' => 'secret'],
        ['name' => 'Carol', 'email' => 'carol@test.com', 'password' => 'secret'],
        ['name' => 'Dave',  'email' => 'dave@test.com',  'password' => 'secret'],
    ];

    DataPipeline::register('resume_test', function () use ($rows) {
        return DataPipeline::from($rows)
            ->chunk(2)
            ->validate(['email' => 'required|email'])
            ->insert(User::class);
    });

    $run = PipelineRun::create([
        'name' => 'resume_test',
        'status' => 'FAILED',
        'source_type' => 'ArraySource',
        'total_records' => 4,
        'processed_records' => 2,
        'successful_records' => 2,
        'failed_records' => 0,
        'started_at' => now(),
    ]);

    // Pre-seed User for Alice & Bob (chunk 1)
    User::create(['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret']);
    User::create(['name' => 'Bob', 'email' => 'bob@test.com', 'password' => 'secret']);

    // Chunk 1 was already COMPLETED
    PipelineChunk::create([
        'pipeline_id' => $run->id,
        'sequence' => 1,
        'status' => 'COMPLETED',
        'records_count' => 2,
        'successful_count' => 2,
        'failed_count' => 0,
        'last_record_offset' => 2,
    ]);

    $this->artisan('pipeline:resume', ['id' => $run->id])
        ->expectsOutputToContain('Resuming execution for Pipeline [resume_test]')
        ->expectsOutputToContain('Pipeline [resume_test] resumed successfully!')
        ->assertSuccessful();

    // Now all 4 users exist (Carol & Dave were processed in chunk 2 without duplicating Alice & Bob)
    expect(User::count())->toBe(4)
        ->and(User::where('email', 'carol@test.com')->exists())->toBeTrue()
        ->and(User::where('email', 'dave@test.com')->exists())->toBeTrue();

    expect($run->fresh()->status)->toBe('COMPLETED');
});

it('surgically retries failed records via pipeline:retry-failed, preserving failures and recovering successes', function () {
    DataPipeline::register('retry_test', function () {
        return DataPipeline::from([])
            ->validate(['email' => ['required', 'email']])
            ->insert(User::class);
    });

    $run = PipelineRun::create([
        'name' => 'retry_test',
        'status' => 'PARTIAL_SUCCESS',
        'source_type' => 'ArraySource',
        'total_records' => 3,
        'processed_records' => 3,
        'successful_records' => 1,
        'failed_records' => 2,
        'started_at' => now(),
    ]);

    // Error 1: fixed valid payload that should now succeed
    $err1 = PipelineError::create([
        'pipeline_id' => $run->id,
        'record_index' => 1,
        'record_identifier' => 'rec-1',
        'raw_payload' => ['name' => 'Valid User', 'email' => 'valid@test.com', 'password' => 'secret'],
        'validation_errors' => ['email' => ['Invalid email']],
    ]);

    // Error 2: still invalid payload that should remain in quarantine
    $err2 = PipelineError::create([
        'pipeline_id' => $run->id,
        'record_index' => 2,
        'record_identifier' => 'rec-2',
        'raw_payload' => ['name' => 'Still Bad', 'email' => 'not-an-email', 'password' => 'secret'],
        'validation_errors' => ['email' => ['Invalid email']],
    ]);

    $this->artisan('pipeline:retry-failed', ['id' => $run->id])
        ->expectsOutputToContain('Found 2 quarantined record(s) for Pipeline [retry_test]')
        ->expectsOutputToContain('Surgical retry completed for Pipeline [retry_test]!')
        ->assertSuccessful();

    // Succeeded record was inserted into database
    expect(User::where('email', 'valid@test.com')->exists())->toBeTrue();

    // Succeeded error was removed from quarantine
    expect(PipelineError::find($err1->id))->toBeNull();

    // Failing record was PRESERVED in quarantine!
    expect(PipelineError::find($err2->id))->not->toBeNull()
        ->and(PipelineError::where('pipeline_id', $run->id)->count())->toBe(1);

    // Progress counters updated
    $freshRun = $run->fresh();
    expect($freshRun->successful_records)->toBe(2)
        ->and($freshRun->failed_records)->toBe(1)
        ->and($freshRun->status)->toBe('PARTIAL_SUCCESS');
});
