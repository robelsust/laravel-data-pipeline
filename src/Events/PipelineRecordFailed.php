<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Events;

use Illuminate\Foundation\Events\Dispatchable;
use Robel\DataPipeline\Results\RecordFailure;

final readonly class PipelineRecordFailed
{
    use Dispatchable;

    public function __construct(
        public ?string $runId,
        public RecordFailure $failure,
    ) {}
}
