<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Cache;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(RefreshDatabase::class);

it('guarantees zero database writes during preview even with autoCreate callbacks', function () {
    expect(User::count())->toBe(0);

    $rows = [
        ['category' => 'Engineering', 'title' => 'Dev 1'],
        ['category' => 'Marketing',   'title' => 'Marketer 1'],
    ];

    $previewRecords = DataPipeline::from($rows)
        ->resolveRelation(
            foreignKey: 'user_id',
            lookupColumn: 'category',
            target: 'users',
            targetKey: 'name',
            required: true,
            autoCreate: fn ($cat) => User::create([
                'name' => $cat,
                'email' => strtolower($cat).'@company.com',
                'password' => 'secret',
            ]),
        )
        ->preview(2);

    expect($previewRecords)->toHaveCount(2)
        ->and($previewRecords[0]->get('user_id'))->toBe('simulated-Engineering')
        ->and($previewRecords[1]->get('user_id'))->toBe('simulated-Marketing');

    // Crucial check: zero rows written to database
    expect(User::count())->toBe(0);
});

it('guarantees zero database writes during dryRun across the entire dataset', function () {
    expect(User::count())->toBe(0);

    $rows = [
        ['category' => 'HR',      'email' => 'hr@test.com'],
        ['category' => 'Finance', 'email' => 'finance@test.com'],
    ];

    $result = DataPipeline::from($rows)
        ->resolveRelation(
            foreignKey: 'user_id',
            lookupColumn: 'category',
            target: 'users',
            targetKey: 'name',
            required: true,
            autoCreate: fn ($cat) => [
                'name' => $cat,
                'email' => strtolower($cat).'@company.com',
                'password' => 'secret',
            ],
        )
        ->insert(User::class)
        ->dryRun();

    expect($result['total'])->toBe(2)
        ->and($result['valid'])->toBe(2)
        ->and($result['invalid'])->toBe(0);

    // Crucial check: zero rows written to database
    expect(User::count())->toBe(0);
});

it('performs simulation-local deduplication during preview while preventing shared-cache writes', function () {
    Cache::store('array')->flush();

    $rows = [
        ['name' => 'Alice First',  'email' => 'preview.dup@test.com', 'password' => 'secret'],
        ['name' => 'Alice Second', 'email' => 'preview.dup@test.com', 'password' => 'secret'],
    ];

    $records = DataPipeline::from($rows)
        ->deduplicate('email', store: 'array', scope: 'preview:test:')
        ->insert(User::class)
        ->preview(2);

    expect($records)->toHaveCount(2)
        ->and($records[0]->isDropped())->toBeFalse()
        ->and($records[1]->isDropped())->toBeTrue();

    // Verify simulation did NOT write to shared cache
    $hash = hash('xxh128', 'preview.dup@test.com');
    expect(Cache::store('array')->has("preview:test:{$hash}"))->toBeFalse();

    // Zero database writes
    expect(User::count())->toBe(0);
});

it('performs simulation-local deduplication during dryRun, reporting accurate valid and dropped counts without shared-cache writes', function () {
    Cache::store('array')->flush();

    $rows = [
        ['name' => 'Bob First',  'email' => 'dryrun.dup@test.com', 'password' => 'secret'],
        ['name' => 'Bob Second', 'email' => 'dryrun.dup@test.com', 'password' => 'secret'],
    ];

    $pipeline = DataPipeline::from($rows)
        ->deduplicate('email', store: 'array', scope: 'dryrun:test:')
        ->insert(User::class);

    $dryRunResult = $pipeline->dryRun();

    // Two identical records must report 1 valid and 1 dropped (not valid: 2, dropped: 0)
    expect($dryRunResult['total'])->toBe(2)
        ->and($dryRunResult['valid'])->toBe(1)
        ->and($dryRunResult['dropped'])->toBe(1)
        ->and($dryRunResult['invalid'])->toBe(0);

    // Shared cache must remain clean
    $hash = hash('xxh128', 'dryrun.dup@test.com');
    expect(Cache::store('array')->has("dryrun:test:{$hash}"))->toBeFalse();
    expect(User::count())->toBe(0);

    // Actual run now runs cleanly and processes only 1 valid record
    $runResult = $pipeline->run();

    expect($runResult->successful)->toBe(1)
        ->and($runResult->failed)->toBe(0)
        ->and(User::where('email', 'dryrun.dup@test.com')->count())->toBe(1);
});
