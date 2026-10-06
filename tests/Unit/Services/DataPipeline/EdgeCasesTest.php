<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Cache;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Robel\DataPipeline\Stages\ResolveRelationStage;

uses(RefreshDatabase::class);

it('prevents composite deduplication delimiter collision for keys containing delimiters', function () {
    $stage = new DeduplicateStage(['first', 'second'], action: 'drop');

    $rec1 = RecordContext::fromRaw(['first' => 'x:y', 'second' => 'z'], 1);
    $rec2 = RecordContext::fromRaw(['first' => 'x',   'second' => 'y:z'], 2);

    $stage->handleRecord($rec1);
    $stage->handleRecord($rec2);

    // Neither record should be dropped because they are distinct tuples
    expect($rec1->isDropped())->toBeFalse()
        ->and($rec2->isDropped())->toBeFalse();
});

it('flags validation errors when required lookup column is completely missing across all chunk records', function () {
    $records = [
        RecordContext::fromRaw(['name' => 'Item 1', 'category' => null], 1),
        RecordContext::fromRaw(['name' => 'Item 2', 'category' => ''], 2),
    ];

    $chunk = new RecordChunk($records, 1);
    $stage = new ResolveRelationStage(
        foreignKey: 'category_id',
        lookupColumn: 'category',
        target: 'users',
        targetKey: 'name',
        required: true,
    );

    $processed = $stage->handleChunk($chunk);

    // Both records MUST have errors recorded despite the chunk having zero non-empty lookups
    expect($processed->invalid())->toHaveCount(2);
    expect($records[0]->isInvalid())->toBeTrue()
        ->and($records[0]->errors())->toHaveKey('category_id');
    expect($records[1]->isInvalid())->toBeTrue()
        ->and($records[1]->errors())->toHaveKey('category_id');
});

it('supports distributed deduplication across worker processes using cache store', function () {
    Cache::store('array')->flush();

    // Stage in Worker 1
    $worker1Stage = new DeduplicateStage('email', action: 'drop', store: 'array', scope: 'pipeline:test:');
    $rec1 = RecordContext::fromRaw(['email' => 'worker@test.com'], 1);
    $worker1Stage->handleRecord($rec1);
    expect($rec1->isDropped())->toBeFalse();
    $worker1Stage->commitClaims();

    // Stage in Worker 2 (completely new instance in separate memory)
    $worker2Stage = new DeduplicateStage('email', action: 'drop', store: 'array', scope: 'pipeline:test:');
    $rec2 = RecordContext::fromRaw(['email' => 'worker@test.com'], 2);
    $worker2Stage->handleRecord($rec2);

    // Worker 2 drops the duplicate via shared cache
    expect($rec2->isDropped())->toBeTrue();
});
