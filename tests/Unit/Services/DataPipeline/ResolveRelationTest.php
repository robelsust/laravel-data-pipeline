<?php

declare(strict_types=1);

use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Stages\ResolveRelationStage;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

uses(RefreshDatabase::class);

beforeEach(function () {
    Schema::create('categories', function (Blueprint $table) {
        $table->id();
        $table->string('name')->unique();
        $table->timestamps();
    });

    DB::table('categories')->insert([
        ['id' => 10, 'name' => 'Electronics', 'created_at' => now(), 'updated_at' => now()],
        ['id' => 20, 'name' => 'Books',       'created_at' => now(), 'updated_at' => now()],
    ]);
});

it('resolves foreign keys using a single batched query per chunk', function () {
    $r1 = RecordContext::fromRaw(['product' => 'Phone', 'cat_name' => 'Electronics'], 1);
    $r2 = RecordContext::fromRaw(['product' => 'Laptop', 'cat_name' => 'Electronics'], 2);
    $r3 = RecordContext::fromRaw(['product' => 'Novel', 'cat_name' => 'Books'], 3);

    $chunk = new RecordChunk([$r1, $r2, $r3]);

    $stage = new ResolveRelationStage(
        foreignKey: 'category_id',
        lookupColumn: 'cat_name',
        target: 'categories',
        targetKey: 'name',
        primaryKey: 'id',
    );

    DB::enableQueryLog();
    $stage->handleChunk($chunk);
    $queries = DB::getQueryLog();
    DB::disableQueryLog();

    // Exactly 1 single query for the entire chunk
    expect($queries)->toHaveCount(1)
        ->and($queries[0]['query'])->toContain('select "id", "name" from "categories" where "name" in')
        ->and($r1->get('category_id'))->toBe(10)
        ->and($r2->get('category_id'))->toBe(10)
        ->and($r3->get('category_id'))->toBe(20)
        ->and($r1->isValid())->toBeTrue();
});

it('marks record as invalid when required relation is missing', function () {
    $r1 = RecordContext::fromRaw(['product' => 'Alien Artifact', 'cat_name' => 'SciFiUnknown'], 1);
    $chunk = new RecordChunk([$r1]);

    $stage = new ResolveRelationStage(
        foreignKey: 'category_id',
        lookupColumn: 'cat_name',
        target: 'categories',
        targetKey: 'name',
        required: true,
    );

    $stage->handleChunk($chunk);

    expect($r1->isValid())->toBeFalse()
        ->and($r1->errors()['category_id'][0])->toContain("Could not resolve category_id for 'SciFiUnknown'");
});

it('supports auto-creating missing relations on the fly', function () {
    $r1 = RecordContext::fromRaw(['product' => 'Shirt', 'cat_name' => 'Apparel'], 1);
    $chunk = new RecordChunk([$r1]);

    $stage = new ResolveRelationStage(
        foreignKey: 'category_id',
        lookupColumn: 'cat_name',
        target: 'categories',
        targetKey: 'name',
        required: true,
        autoCreate: function (string $missingName) {
            return DB::table('categories')->insertGetId([
                'name' => $missingName,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    );

    $stage->handleChunk($chunk);

    expect($r1->isValid())->toBeTrue()
        ->and($r1->get('category_id'))->not->toBeNull()
        ->and(DB::table('categories')->where('name', 'Apparel')->exists())->toBeTrue();
});

it('integrates seamlessly in DataPipeline fluent builder', function () {
    $rows = [
        ['title' => 'iPhone', 'category' => 'Electronics'],
        ['title' => 'War & Peace', 'category' => 'Books'],
    ];

    $preview = DataPipeline::from($rows)
        ->resolveRelation(
            foreignKey: 'category_id',
            lookupColumn: 'category',
            target: 'categories',
            targetKey: 'name'
        )
        ->preview(2);

    expect($preview->first()->get('category_id'))->toBe(10)
        ->and($preview->last()->get('category_id'))->toBe(20);
});
