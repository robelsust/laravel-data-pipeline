<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Validation\BatchValidator;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;

uses(RefreshDatabase::class);

it('validates standard rules across a chunk of records', function () {
    $validator = new BatchValidator([
        'name' => ['required', 'string'],
        'email' => ['required', 'email'],
    ]);

    $r1 = RecordContext::fromRaw(['name' => 'Alice', 'email' => 'alice@example.com'], 1);
    $r2 = RecordContext::fromRaw(['name' => '', 'email' => 'not-an-email'], 2);

    $chunk = new RecordChunk([$r1, $r2]);
    $validator->validateChunk($chunk);

    expect($r1->isValid())->toBeTrue()
        ->and($r2->isValid())->toBeFalse()
        ->and($r2->errors())->toHaveKeys(['name', 'email']);
});

it('eliminates N+1 queries by batching database unique checks in a single query', function () {
    // Seed an existing user
    User::factory()->create(['email' => 'existing@example.com']);

    $validator = new BatchValidator([
        'email' => ['required', 'email', 'unique:users,email'],
    ]);

    // Create 3 records:
    // 1. Valid new email
    // 2. Email that already exists in DB
    // 3. Email that is duplicated within the chunk itself
    $r1 = RecordContext::fromRaw(['email' => 'unique1@example.com'], 1);
    $r2 = RecordContext::fromRaw(['email' => 'existing@example.com'], 2);
    $r3 = RecordContext::fromRaw(['email' => 'unique1@example.com'], 3); // duplicate of r1

    $chunk = new RecordChunk([$r1, $r2, $r3]);

    // Count DB queries during chunk validation
    DB::enableQueryLog();
    $validator->validateChunk($chunk);
    $queries = DB::getQueryLog();
    DB::disableQueryLog();

    // Exactly 1 query executed for unique check across all records in the chunk!
    expect($queries)->toHaveCount(1)
        ->and($queries[0]['query'])->toContain('select "email" from "users" where "email" in')
        ->and($r1->isValid())->toBeTrue()
        ->and($r2->isValid())->toBeFalse()
        ->and($r2->errors()['email'][0])->toContain('already been taken')
        ->and($r3->isValid())->toBeFalse()
        ->and($r3->errors()['email'][0])->toContain('duplicated within the current batch');
});

it('eliminates N+1 queries by batching database exists checks in a single query', function () {
    User::factory()->create(['id' => 10]);
    User::factory()->create(['id' => 20]);

    $validator = new BatchValidator([
        'user_id' => ['required', 'exists:users,id'],
    ]);

    $r1 = RecordContext::fromRaw(['user_id' => 10], 1);
    $r2 = RecordContext::fromRaw(['user_id' => 20], 2);
    $r3 = RecordContext::fromRaw(['user_id' => 999], 3); // non-existent

    $chunk = new RecordChunk([$r1, $r2, $r3]);

    DB::enableQueryLog();
    $validator->validateChunk($chunk);
    $queries = DB::getQueryLog();
    DB::disableQueryLog();

    // Exactly 1 query executed for exists check across all records in the chunk!
    expect($queries)->toHaveCount(1)
        ->and($queries[0]['query'])->toContain('select "id" from "users" where "id" in')
        ->and($r1->isValid())->toBeTrue()
        ->and($r2->isValid())->toBeTrue()
        ->and($r3->isValid())->toBeFalse()
        ->and($r3->errors()['user_id'][0])->toContain('is invalid');
});
