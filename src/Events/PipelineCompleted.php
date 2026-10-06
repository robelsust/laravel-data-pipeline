<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Events;

use Illuminate\Foundation\Events\Dispatchable;
use Robel\DataPipeline\Results\PipelineResult;

final readonly class PipelineCompleted
{
    use Dispatchable;

    public function __construct(
        public ?string $runId,
        public PipelineResult $result,
    ) {}
}
