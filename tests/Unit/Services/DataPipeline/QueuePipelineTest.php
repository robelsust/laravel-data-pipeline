<?php

declare(strict_types=1);

use Illuminate\Bus\PendingBatch;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Bus;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Stages\TransformStage;
use Robel\DataPipeline\Stages\ValidateStage;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(RefreshDatabase::class);

it('dispatches queue batches with ProcessPipelineChunkJobs', function () {
    Bus::fake();

    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com'],
        ['name' => 'Bob',   'email' => 'bob@test.com'],
        ['name' => 'Carol', 'email' => 'carol@test.com'],
    ];

    $runId = DataPipeline::from($rows)
        ->name('Queued Import')
        ->chunk(2)
        ->onQueue('high-priority')
        ->validate(['email' => ['required', 'email']])
        ->insert(User::class)
        ->queue();

    expect($runId)->not->toBeNull();

    // Verify run record created
    $run = PipelineRun::find($runId);
    expect($run)->not->toBeNull()
        ->and($run->name)->toBe('Queued Import')
        ->and($run->total_records)->toBe(3);

    // Verify Bus batch dispatched with 2 chunk jobs (chunk size 2 across 3 records = 2 chunks)
    Bus::assertBatched(function (PendingBatch $batch) {
        return $batch->name === 'Queued Import'
            && $batch->queue() === 'high-priority'
            && $batch->jobs->count() === 2;
    });
});

it('processes a chunk job and writes to sink and repository', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 2,
        name: 'Worker Execution Test'
    );

    $rawRecords = [
        ['index' => 1, 'raw' => ['name' => 'Dave', 'email' => 'dave@test.com', 'password' => 'my-secret-1']],
        ['index' => 2, 'raw' => ['name' => '',     'email' => 'invalid-email',  'password' => 'my-secret-2']],
    ];

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: $rawRecords,
        stages: [
            new TransformStage(fn ($r) => [...$r, 'password' => 'secret']),
            new ValidateStage(['name' => 'required', 'email' => 'email']),
        ],
        sink: new InsertSink(User::class),
        redactedAttributes: ['password'],
    );

    $job->handle($repo);

    // 1 valid user inserted into database
    expect(User::count())->toBe(1)
        ->and(User::where('email', 'dave@test.com')->exists())->toBeTrue();

    // Run progress updated
    $updatedRun = $repo->findRun($run->id);
    expect($updatedRun->processed_records)->toBe(2)
        ->and($updatedRun->successful_records)->toBe(1)
        ->and($updatedRun->failed_records)->toBe(1);

    // Quarantined error saved
    $quarantined = $repo->getQuarantinedRecords($run->id);
    expect($quarantined)->toHaveCount(1)
        ->and($quarantined[2]['email'])->toBe('invalid-email')
        ->and($quarantined[2]['password'])->toBe('[REDACTED]');
});
