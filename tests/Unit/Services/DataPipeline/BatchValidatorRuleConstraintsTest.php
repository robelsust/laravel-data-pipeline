<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Validation\BatchValidator;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Validation\Rule;

uses(RefreshDatabase::class);

it('preserves ignore constraints on unique rules so existing records are not falsely flagged as duplicates', function () {
    $existing = User::create([
        'name' => 'Existing User',
        'email' => 'existing@test.com',
        'password' => 'secret',
    ]);

    $records = [
        RecordContext::fromRaw(['id' => $existing->id, 'email' => 'existing@test.com'], 1),
    ];

    $chunk = new RecordChunk($records, 1);
    $validator = new BatchValidator([
        'email' => [Rule::unique('users', 'email')->ignore($existing->id)],
    ]);

    $validator->validateChunk($chunk);

    // Should NOT be invalid because the ID was ignored
    expect($records[0]->isValid())->toBeTrue()
        ->and($records[0]->errors())->toBeEmpty();
});

it('preserves ignoreModel constraints when Model instance is passed to unique rule', function () {
    $existing = User::create([
        'name' => 'Existing User 2',
        'email' => 'model-ignore@test.com',
        'password' => 'secret',
    ]);

    $records = [
        RecordContext::fromRaw(['id' => $existing->id, 'email' => 'model-ignore@test.com'], 1),
    ];

    $chunk = new RecordChunk($records, 1);
    $validator = new BatchValidator([
        'email' => [Rule::unique('users', 'email')->ignore($existing)],
    ]);

    $validator->validateChunk($chunk);

    expect($records[0]->isValid())->toBeTrue();
});

it('enforces scoped where conditions on exists rules so unmatched scopes fail validation', function () {
    $activeUser = User::create([
        'name' => 'Active Account',
        'email' => 'active@test.com',
        'password' => 'secret',
    ]);

    $inactiveUser = User::create([
        'name' => 'Inactive Account',
        'email' => 'inactive@test.com',
        'password' => 'secret',
    ]);

    $records = [
        RecordContext::fromRaw(['user_id' => $activeUser->id], 1),
        RecordContext::fromRaw(['user_id' => $inactiveUser->id], 2),
    ];

    $chunk = new RecordChunk($records, 1);
    $validator = new BatchValidator([
        'user_id' => [
            Rule::exists('users', 'id')->where('name', 'Active Account'),
        ],
    ]);

    $validator->validateChunk($chunk);

    // Active user matches scoped where condition
    expect($records[0]->isValid())->toBeTrue();

    // Inactive user fails scoped where condition
    expect($records[1]->isInvalid())->toBeTrue()
        ->and($records[1]->errors())->toHaveKey('user_id');
});

it('enforces closure-based using conditions on exists rules', function () {
    $activeUser = User::create([
        'name' => 'VIP',
        'email' => 'vip@test.com',
        'password' => 'secret',
    ]);

    $standardUser = User::create([
        'name' => 'Standard',
        'email' => 'standard@test.com',
        'password' => 'secret',
    ]);

    $records = [
        RecordContext::fromRaw(['user_id' => $activeUser->id], 1),
        RecordContext::fromRaw(['user_id' => $standardUser->id], 2),
    ];

    $chunk = new RecordChunk($records, 1);
    $validator = new BatchValidator([
        'user_id' => [
            Rule::exists('users', 'id')->where(function ($query) {
                $query->where('name', 'VIP');
            }),
        ],
    ]);

    $validator->validateChunk($chunk);

    expect($records[0]->isValid())->toBeTrue();
    expect($records[1]->isInvalid())->toBeTrue();
});

it('correctly parses ignore parameter from string formatted unique rules', function () {
    $existing = User::create([
        'name' => 'String Ignore',
        'email' => 'str-ignore@test.com',
        'password' => 'secret',
    ]);

    $records = [
        RecordContext::fromRaw(['id' => $existing->id, 'email' => 'str-ignore@test.com'], 1),
    ];

    $chunk = new RecordChunk($records, 1);
    $validator = new BatchValidator([
        'email' => "unique:users,email,{$existing->id},id",
    ]);

    $validator->validateChunk($chunk);

    expect($records[0]->isValid())->toBeTrue();
});
