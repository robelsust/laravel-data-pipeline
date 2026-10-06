<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Events;

use Illuminate\Foundation\Events\Dispatchable;
use Throwable;

final readonly class PipelineFailed
{
    use Dispatchable;

    public function __construct(
        public ?string $runId,
        public Throwable $exception,
    ) {}
}
