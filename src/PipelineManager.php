<?php

declare(strict_types=1);

namespace Robel\DataPipeline;

use Closure;
use Robel\DataPipeline\Results\PipelineResult;

final class PipelineManager
{
    public function from(mixed $source): DataPipeline
    {
        return DataPipeline::from($source);
    }

    public function register(string $name, Closure|string $definition): void
    {
        DataPipeline::register($name, $definition);
    }

    public function resolveDefinition(string $name): ?DataPipeline
    {
        return DataPipeline::resolveDefinition($name);
    }

    public function resumeRun(string $runId, ?DataPipeline $pipeline = null): PipelineResult
    {
        return DataPipeline::resumeRun($runId, $pipeline);
    }

    /** @return array{retried: int, recovered: int, remaining_failed: int} */
    public function retryFailedRecords(string $runId, ?DataPipeline $pipeline = null): array
    {
        return DataPipeline::retryFailedRecords($runId, $pipeline);
    }
}
