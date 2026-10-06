<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Robel\DataPipeline\Stages\FilterStage;
use Robel\DataPipeline\Stages\ResolveRelationStage;
use Robel\DataPipeline\Stages\TransformStage;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(RefreshDatabase::class);

it('successfully serializes and unserializes ProcessPipelineChunkJob with closure stages without errors', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Closure Serialization Test'
    );

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Alice', 'email' => 'alice@test.com', 'category_name' => 'Tech']],
        ],
        stages: [
            new TransformStage(fn ($data) => [...$data, 'transformed' => true]),
            new FilterStage(fn ($data) => ! empty($data['email'])),
            new DeduplicateStage(fn ($data) => (string) $data['email']),
            new ResolveRelationStage(
                foreignKey: 'remember_token',
                lookupColumn: 'category_name',
                target: 'users',
                targetKey: 'name',
                required: false,
                autoCreate: fn ($val) => 'token-123',
            ),
            new TransformStage(fn ($data) => [
                'name' => $data['name'],
                'email' => $data['email'],
                'password' => 'secret',
                'remember_token' => $data['remember_token'] ?? null,
            ]),
        ],
        sink: new InsertSink(User::class),
        redactedAttributes: ['password'],
    );

    // Serialization must NOT throw "Serialization of 'Closure' is not allowed"
    $serialized = serialize($job);
    expect($serialized)->toBeString()->and(strlen($serialized))->toBeGreaterThan(0);

    // Unserialization restores operational job
    /** @var ProcessPipelineChunkJob $unserialized */
    $unserialized = unserialize($serialized);
    expect($unserialized)->toBeInstanceOf(ProcessPipelineChunkJob::class);

    // Execute handle on unserialized job
    $unserialized->handle($repo);

    expect(User::where('email', 'alice@test.com')->exists())->toBeTrue()
        ->and(User::count())->toBe(1);

    $chunk = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunk)->not->toBeNull()
        ->and($chunk->status)->toBe('COMPLETED');
});

it('guarantees idempotency when the same chunk job is delivered multiple times', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Idempotency Test'
    );

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['name' => 'Idempotent User', 'email' => 'idem@test.com', 'password' => 'secret']],
        ],
        stages: [
            new TransformStage(fn ($data) => $data),
        ],
        sink: new InsertSink(User::class),
    );

    // First execution
    $job->handle($repo);

    expect(User::count())->toBe(1);
    $runFresh = PipelineRun::find($run->id);
    expect($runFresh->processed_records)->toBe(1)
        ->and($runFresh->successful_records)->toBe(1)
        ->and($runFresh->failed_records)->toBe(0);

    $chunk = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunk->status)->toBe('COMPLETED');

    // Duplicate delivery of the exact same job (at-least-once queue redelivery)
    $job->handle($repo);

    // Assert User count is still 1
    expect(User::count())->toBe(1);

    // Assert counters were NOT double incremented
    $runFreshAfter = PipelineRun::find($run->id);
    expect($runFreshAfter->processed_records)->toBe(1)
        ->and($runFreshAfter->successful_records)->toBe(1)
        ->and($runFreshAfter->failed_records)->toBe(0);

    // Assert chunk status was NOT corrupted or overwritten
    $chunkAfter = PipelineChunk::where('pipeline_id', $run->id)->where('sequence', 1)->first();
    expect($chunkAfter->status)->toBe('COMPLETED')
        ->and($chunkAfter->successful_count)->toBe(1);
});
