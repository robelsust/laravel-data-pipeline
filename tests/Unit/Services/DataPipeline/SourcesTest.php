<?php

declare(strict_types=1);

use Robel\DataPipeline\Sources\ArraySource;
use Robel\DataPipeline\Sources\CsvSource;
use Robel\DataPipeline\Sources\LazySource;
use Illuminate\Support\LazyCollection;

it('iterates through ArraySource correctly', function () {
    $data = [
        ['name' => 'John', 'age' => 30],
        ['name' => 'Jane', 'age' => 25],
    ];

    $source = new ArraySource($data);

    expect($source->count())->toBe(2)
        ->and(iterator_to_array($source->records()))->toBe($data)
        ->and($source->fingerprint())->toContain('array:');
});

it('streams through LazySource without buffering', function () {
    $generator = function () {
        yield 1 => ['id' => 10, 'title' => 'Item 10'];
        yield 2 => ['id' => 20, 'title' => 'Item 20'];
    };

    $source = new LazySource(new LazyCollection($generator));

    $records = [];
    foreach ($source->records() as $key => $record) {
        $records[$key] = $record;
    }

    expect($records)->toHaveCount(2)
        ->and($records[1]['title'])->toBe('Item 10')
        ->and($records[2]['title'])->toBe('Item 20');
});

it('parses CSV files accurately with CsvSource', function () {
    $tempCsv = tempnam(sys_get_temp_dir(), 'test_csv_').'.csv';
    file_put_contents($tempCsv, "First Name,Email\nJohn,john@example.com\nJane,jane@example.com\n");

    try {
        $source = new CsvSource($tempCsv);
        $records = [];
        foreach ($source->records() as $line => $record) {
            $records[$line] = $record;
        }

        expect($records)->toHaveCount(2)
            ->and($records[1])->toBe(['First Name' => 'John', 'Email' => 'john@example.com'])
            ->and($records[2])->toBe(['First Name' => 'Jane', 'Email' => 'jane@example.com']);
    } finally {
        @unlink($tempCsv);
    }
});
