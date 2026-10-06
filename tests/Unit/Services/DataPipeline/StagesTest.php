<?php

declare(strict_types=1);

use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Robel\DataPipeline\Stages\FilterStage;
use Robel\DataPipeline\Stages\MapFieldsStage;
use Robel\DataPipeline\Stages\TransformStage;

it('renames keys with MapFieldsStage', function () {
    $stage = new MapFieldsStage([
        'Full Name' => 'name',
        'Email Address' => 'email',
    ]);

    $record = RecordContext::fromRaw([
        'Full Name' => 'Alice Smith',
        'Email Address' => 'alice@test.com',
        'age' => 30,
    ], 1);

    $stage->handleRecord($record);

    expect($record->data())->toBe([
        'name' => 'Alice Smith',
        'email' => 'alice@test.com',
        'age' => 30,
    ]);
});

it('transforms records using closure or array mapping with TransformStage', function () {
    // 1. Array mapping per field
    $stage = new TransformStage([
        'email' => fn ($val) => strtolower($val),
        'score' => fn ($val) => $val * 2,
    ]);

    $record = RecordContext::fromRaw([
        'email' => 'ALICE@EXAMPLE.COM',
        'score' => 50,
    ], 1);

    $stage->handleRecord($record);

    expect($record->get('email'))->toBe('alice@example.com')
        ->and($record->get('score'))->toBe(100);

    // 2. Full record closure
    $closureStage = new TransformStage(function (array $data) {
        $data['computed'] = true;

        return $data;
    });

    $closureStage->handleRecord($record);
    expect($record->get('computed'))->toBeTrue();
});

it('drops non-matching records with FilterStage', function () {
    $stage = new FilterStage(fn (array $data) => $data['active'] === true);

    $activeRecord = RecordContext::fromRaw(['id' => 1, 'active' => true], 1);
    $inactiveRecord = RecordContext::fromRaw(['id' => 2, 'active' => false], 2);

    $stage->handleRecord($activeRecord);
    $stage->handleRecord($inactiveRecord);

    expect($activeRecord->isDropped())->toBeFalse()
        ->and($inactiveRecord->isDropped())->toBeTrue();
});

it('deduplicates records by a single key dropping subsequent duplicates', function () {
    $stage = new DeduplicateStage('email');

    $r1 = RecordContext::fromRaw(['id' => 1, 'email' => 'test@example.com'], 1);
    $r2 = RecordContext::fromRaw(['id' => 2, 'email' => 'test@example.com'], 2);
    $r3 = RecordContext::fromRaw(['id' => 3, 'email' => 'other@example.com'], 3);

    $stage->handleRecord($r1);
    $stage->handleRecord($r2);
    $stage->handleRecord($r3);

    expect($r1->isDropped())->toBeFalse()
        ->and($r2->isDropped())->toBeTrue()
        ->and($r3->isDropped())->toBeFalse()
        ->and($stage->seenCount())->toBe(2);
});

it('deduplicates records by composite keys', function () {
    $stage = new DeduplicateStage(['tenant_id', 'slug']);

    $r1 = RecordContext::fromRaw(['tenant_id' => 1, 'slug' => 'post-a'], 1);
    $r2 = RecordContext::fromRaw(['tenant_id' => 2, 'slug' => 'post-a'], 2);
    $r3 = RecordContext::fromRaw(['tenant_id' => 1, 'slug' => 'post-a'], 3);

    $stage->handleRecord($r1);
    $stage->handleRecord($r2);
    $stage->handleRecord($r3);

    expect($r1->isDropped())->toBeFalse()
        ->and($r2->isDropped())->toBeFalse()
        ->and($r3->isDropped())->toBeTrue();
});

it('deduplicates records using custom closure key', function () {
    $stage = new DeduplicateStage(
        fn (array $data) => strtolower($data['code'] ?? '')
    );

    $r1 = RecordContext::fromRaw(['code' => 'ABC'], 1);
    $r2 = RecordContext::fromRaw(['code' => 'abc'], 2);

    $stage->handleRecord($r1);
    $stage->handleRecord($r2);

    expect($r1->isDropped())->toBeFalse()
        ->and($r2->isDropped())->toBeTrue();
});

it('flags duplicate validation error when deduplicate action is error', function () {
    $stage = new DeduplicateStage('email', action: 'error');

    $r1 = RecordContext::fromRaw(['email' => 'alice@test.com'], 1);
    $r2 = RecordContext::fromRaw(['email' => 'alice@test.com'], 2);

    $stage->handleRecord($r1);
    $stage->handleRecord($r2);

    expect($r1->isValid())->toBeTrue()
        ->and($r2->isInvalid())->toBeTrue()
        ->and($r2->errors())->toHaveKey('_duplicate');
});
