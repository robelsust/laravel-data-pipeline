<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Facades;

use Illuminate\Support\Facades\Facade;
use Robel\DataPipeline\DataPipeline as PipelineService;

/**
 * @method static PipelineService from(mixed $source)
 * @method static void register(string $name, \Closure|string $definition)
 * @method static ?PipelineService resolveDefinition(string $name)
 * @method static \Robel\DataPipeline\Results\PipelineResult resumeRun(string $runId, ?PipelineService $pipeline = null)
 * @method static array retryFailedRecords(string $runId, ?PipelineService $pipeline = null)
 *
 * @see PipelineService
 */
class DataPipeline extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'data-pipeline';
    }
}
