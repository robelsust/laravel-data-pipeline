<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Sinks\CustomSink;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Sinks\UpsertSink;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

it('inserts valid records via InsertSink', function () {
    $sink = new InsertSink(User::class);

    $r1 = RecordContext::fromRaw(['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'], 1);
    $r2 = RecordContext::fromRaw(['name' => 'Bob', 'email' => 'bob@test.com', 'password' => 'secret'], 2);

    $chunk = new RecordChunk([$r1, $r2]);
    $result = $sink->persist($chunk);

    expect($result->persistedCount)->toBe(2)
        ->and(User::count())->toBe(2)
        ->and(User::where('email', 'alice@test.com')->exists())->toBeTrue();
});

it('upserts and updates conflicting records via UpsertSink', function () {
    // Seed initial user
    User::factory()->create([
        'name' => 'Old Alice',
        'email' => 'alice@test.com',
    ]);

    $sink = new UpsertSink(User::class, uniqueBy: ['email'], update: ['name']);

    $r1 = RecordContext::fromRaw(['name' => 'New Alice', 'email' => 'alice@test.com', 'password' => 'new-secret'], 1);
    $r2 = RecordContext::fromRaw(['name' => 'Bob', 'email' => 'bob@test.com', 'password' => 'secret'], 2);

    $chunk = new RecordChunk([$r1, $r2]);
    $result = $sink->persist($chunk);

    expect($result->persistedCount)->toBe(2)
        ->and(User::count())->toBe(2)
        ->and(User::where('email', 'alice@test.com')->first()->name)->toBe('New Alice');
});

it('invokes custom closure callback with CustomSink', function () {
    $persisted = [];

    $sink = new CustomSink(function (RecordChunk $chunk) use (&$persisted) {
        foreach ($chunk->validPayloads() as $payload) {
            $persisted[] = $payload['email'];
        }
    });

    $r1 = RecordContext::fromRaw(['email' => 'one@test.com'], 1);
    $r2 = RecordContext::fromRaw(['email' => 'two@test.com'], 2);

    $chunk = new RecordChunk([$r1, $r2]);
    $result = $sink->persist($chunk);

    expect($result->persistedCount)->toBe(2)
        ->and($persisted)->toBe(['one@test.com', 'two@test.com']);
});
