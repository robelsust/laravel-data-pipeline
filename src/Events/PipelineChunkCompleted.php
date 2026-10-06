<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Events;

use Illuminate\Foundation\Events\Dispatchable;

final readonly class PipelineChunkCompleted
{
    use Dispatchable;

    public function __construct(
        public ?string $runId,
        public int $sequence,
        public int $recordsCount,
        public int $successfulCount,
        public int $failedCount,
        public float $durationInSeconds = 0.0,
    ) {}
}
