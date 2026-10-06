<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Sinks\CustomSink;
use Robel\DataPipeline\Stages\ResolveRelationStage;
use Robel\DataPipeline\Stages\TransformStage;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(RefreshDatabase::class);

it('preserves exact fluent registration order between record stages and chunk stages', function () {
    $existing = User::create([
        'name' => 'Tech',
        'email' => 'tech@test.com',
        'password' => 'secret',
    ]);

    $rows = [
        ['category_name' => 'Tech'],
    ];

    $capturedRecords = [];

    $result = DataPipeline::from($rows)
        // Stage 1: Record Stage
        ->transform(fn ($data) => [...$data, 'step1' => 'completed'])
        // Stage 2: Chunk Stage (resolves category_id from database)
        ->resolveRelation(
            foreignKey: 'category_id',
            lookupColumn: 'category_name',
            target: 'users',
            targetKey: 'name',
            required: true,
        )
        // Stage 3: Record Stage registered AFTER chunk stage - MUST see category_id from Stage 2!
        ->transform(function ($data) {
            $catId = $data['category_id'] ?? 'MISSING';

            return [...$data, 'formatted_tag' => "TAG-{$catId}"];
        })
        // Stage 4: Record Stage
        ->filter(fn ($data) => ($data['formatted_tag'] ?? '') === "TAG-{$existing->id}")
        ->to(new CustomSink(function ($chunk) use (&$capturedRecords) {
            $capturedRecords = $chunk->records();

            return SinkResult::success(count($capturedRecords));
        }))
        ->run();

    expect($result->successful())->toBe(1);
    expect($capturedRecords)->toHaveCount(1);
    expect($capturedRecords[0]->get('category_id'))->toBe($existing->id);
    expect($capturedRecords[0]->get('formatted_tag'))->toBe("TAG-{$existing->id}");
    expect($capturedRecords[0]->get('step1'))->toBe('completed');
});

it('preserves exact registration order during queued worker execution', function () {
    $existing = User::create([
        'name' => 'Support',
        'email' => 'support@test.com',
        'password' => 'secret',
    ]);

    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 1,
        name: 'Stage Order Queue Test'
    );

    $capturedRecords = [];

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['category_name' => 'Support']],
        ],
        stages: [
            new TransformStage(fn ($data) => [...$data, 'step1' => 'ok']),
            new ResolveRelationStage(
                foreignKey: 'category_id',
                lookupColumn: 'category_name',
                target: 'users',
                targetKey: 'name',
                required: true,
            ),
            new TransformStage(function ($data) {
                return [...$data, 'tag' => 'TAG-'.($data['category_id'] ?? 'NOT_FOUND')];
            }),
        ],
        sink: new CustomSink(function ($chunk) use (&$capturedRecords) {
            $capturedRecords = $chunk->records();

            return SinkResult::success(count($capturedRecords));
        }),
    );

    $job->handle($repo);

    expect($capturedRecords)->toHaveCount(1);
    expect($capturedRecords[0]->get('tag'))->toBe("TAG-{$existing->id}");
});
