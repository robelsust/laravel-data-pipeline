<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\DataPipeline;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;

uses(RefreshDatabase::class);

it('benchmarks DataPipeline query efficiency and memory invariance against naive loop', function () {
    $recordCount = 1000;
    $rows = [];
    for ($i = 1; $i <= $recordCount; $i++) {
        $rows[] = [
            'name' => "User {$i}",
            'email' => "benchmark_user_{$i}@example.com",
            'password' => 'secret',
        ];
    }

    $memoryBefore = memory_get_usage();
    DB::flushQueryLog();
    DB::enableQueryLog();

    $result = DataPipeline::from($rows)
        ->chunk(250)
        ->validate([
            'name' => ['required'],
            'email' => ['required', 'email', 'unique:users,email'],
        ])
        ->upsert(User::class, uniqueBy: ['email'], update: ['name'])
        ->run();

    $queries = DB::getQueryLog();
    DB::disableQueryLog();
    $peakMemoryMb = (memory_get_peak_usage() - $memoryBefore) / (1024 * 1024);

    // 1. Query efficiency: 1,000 records in chunks of 250 = 4 unique checks + 4 upserts = 8 queries!
    // Compared to naive loop which would fire 1,000 SELECT + 1,000 INSERT = 2,000 queries!
    expect(count($queries))->toBeLessThanOrEqual(10)
        ->and($result->successful())->toBe(1000)
        ->and(User::count())->toBe(1000)
        ->and($peakMemoryMb)->toBeLessThan(32.0); // O(1) bounded memory
});
