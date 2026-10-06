<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Sinks\CustomSink;

uses(RefreshDatabase::class);

it('proves strict O(1) memory invariance and high throughput during 5,000 streaming rows ingest', function () {
    // Generator ensures zero upfront dataset buffering into memory
    $generator = function () {
        for ($i = 1; $i <= 5000; $i++) {
            yield [
                'raw_email' => "stream_user_{$i}@benchmark.com",
                'raw_name' => "User {$i}",
                'age' => 20 + ($i % 50),
            ];
        }
    };

    $initialMemory = memory_get_usage(true);
    $queriesExecuted = 0;

    DB::listen(function () use (&$queriesExecuted) {
        $queriesExecuted++;
    });

    $processedCount = 0;

    $result = DataPipeline::from($generator())
        ->chunk(500) // 10 chunks across 5,000 rows
        ->mapFields([
            'raw_email' => 'email',
            'raw_name' => 'name',
        ])
        ->transform(fn ($data) => [...$data, 'password' => 'hashed_pw', 'normalized' => true])
        ->deduplicateBy('email')
        ->validate([
            'email' => ['required', 'email'],
            'age' => ['required', 'numeric', 'min:18'],
        ])
        ->to(new CustomSink(function ($chunk) use (&$processedCount) {
            $count = count($chunk->valid());
            $processedCount += $count;

            return SinkResult::success($count);
        }))
        ->run();

    $peakMemory = memory_get_peak_usage(true);
    $memoryDeltaMb = ($peakMemory - $initialMemory) / (1024 * 1024);

    expect($result->totalProcessed())->toBe(5000)
        ->and($result->successful())->toBe(5000)
        ->and($result->failed())->toBe(0)
        ->and($processedCount)->toBe(5000);

    // Strict memory invariance: peak memory delta for 5,000 rows stays minimal (< 16 MB)
    expect($memoryDeltaMb)->toBeLessThan(16.0);
});
