<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(RefreshDatabase::class);

it('executes pipeline:benchmark command and verifies persistence and cleanup', function () {
    $tempCsv = storage_path('app/test_cmd_benchmark.csv');

    $this->artisan('pipeline:benchmark', [
        '--table' => 'users',
        '--allow-destructive' => true,
        '--records' => 100,
        '--chunk-size' => 25,
        '--file' => $tempCsv,
        '--truncate' => true,
    ])
        ->expectsOutputToContain('Target Table:')
        ->expectsOutputToContain('100 records')
        ->expectsOutputToContain('BENCHMARK VERIFICATION RESULTS')
        ->expectsOutputToContain('Verified O(1) Memory Invariant')
        ->assertSuccessful();

    expect(User::count())->toBe(100)
        ->and(file_exists($tempCsv))->toBeFalse();
});

it('executes pipeline:benchmark with --track and persists checkpoints', function () {
    $tempCsv = storage_path('app/test_cmd_benchmark_track.csv');

    $this->artisan('pipeline:benchmark', [
        '--table' => 'users',
        '--allow-destructive' => true,
        '--records' => 60,
        '--chunk-size' => 20,
        '--file' => $tempCsv,
        '--track' => true,
        '--truncate' => true,
    ])
        ->expectsOutputToContain('pipeline_runs / pipeline_chunks')
        ->assertSuccessful();

    expect(User::count())->toBe(60);

    $run = PipelineRun::latest('started_at')->first();
    expect($run)->not->toBeNull()
        ->and($run->status)->toBe('COMPLETED')
        ->and($run->processed_records)->toBe(60)
        ->and($run->successful_records)->toBe(60);

    $chunks = PipelineChunk::where('pipeline_id', $run->id)->get();
    expect($chunks)->toHaveCount(3);
});

it('executes pipeline:benchmark with --copied-records and --upsert', function () {
    User::create([
        'name' => 'Original User 1',
        'username' => 'user_1',
        'email' => 'bench_user1@example.com',
        'password' => 'secret',
    ]);
    User::create([
        'name' => 'Original User 2',
        'username' => 'user_2',
        'email' => 'bench_user2@example.com',
        'password' => 'secret',
    ]);

    $tempCsv = storage_path('app/test_cmd_benchmark_upsert.csv');

    $this->artisan('pipeline:benchmark', [
        '--table' => 'users',
        '--allow-destructive' => true,
        '--records' => 5,
        '--copied-records' => 2,
        '--chunk-size' => 2,
        '--file' => $tempCsv,
        '--upsert' => true,
        '--unique-by' => 'email',
    ])
        ->expectsOutputToContain('UPSERT (Bulk Merge by email)')
        ->expectsOutputToContain('Existing Rows Updated')
        ->assertSuccessful();

    expect(User::count())->toBe(5)
        ->and(User::where('email', 'bench_user1@example.com')->value('name'))->toBe('Updated Benchmark User 1');
});
