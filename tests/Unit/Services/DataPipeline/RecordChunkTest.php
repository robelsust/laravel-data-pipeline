<?php

declare(strict_types=1);

use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;

it('partitions valid, invalid, and dropped records in a chunk', function () {
    $r1 = RecordContext::fromRaw(['id' => 1, 'email' => 'valid1@test.com'], 1);
    $r2 = RecordContext::fromRaw(['id' => 2, 'email' => 'invalid@test.com'], 2);
    $r2->addError('email', 'Invalid format');
    $r3 = RecordContext::fromRaw(['id' => 3, 'email' => 'valid2@test.com'], 3);
    $r4 = RecordContext::fromRaw(['id' => 4, 'email' => 'dropped@test.com'], 4);
    $r4->drop();

    $chunk = new RecordChunk([$r1, $r2, $r3, $r4], sequence: 2);

    expect($chunk->sequence())->toBe(2)
        ->and($chunk->count())->toBe(4)
        ->and($chunk->valid())->toHaveCount(2)
        ->and($chunk->invalid())->toHaveCount(1)
        ->and($chunk->dropped())->toHaveCount(1)
        ->and($chunk->validPayloads())->toBe([
            ['id' => 1, 'email' => 'valid1@test.com'],
            ['id' => 3, 'email' => 'valid2@test.com'],
        ]);
});
