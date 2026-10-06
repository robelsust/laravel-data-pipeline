<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Results;

use Robel\DataPipeline\RecordChunk;

final readonly class ChunkExecutionResult
{
    /** @param array<int, RecordFailure> $failures */
    public function __construct(
        public RecordChunk $chunk,
        public int $successful,
        public array $failures,
    ) {}

    public function failed(): int
    {
        return count($this->failures);
    }
}
