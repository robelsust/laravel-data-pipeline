<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Events;

use Illuminate\Foundation\Events\Dispatchable;

final readonly class PipelineStarted
{
    use Dispatchable;

    public function __construct(
        public ?string $runId,
        public ?string $name,
        public ?int $totalRecords,
    ) {}
}
