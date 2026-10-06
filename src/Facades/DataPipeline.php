<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Facades;

use Illuminate\Support\Facades\Facade;
use Robel\DataPipeline\Contracts\SourceInterface;
use Robel\DataPipeline\DataPipeline as PipelineService;

/**
 * @method static PipelineService from(SourceInterface|iterable|string $source)
 *
 * @see PipelineService
 */
class DataPipeline extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return PipelineService::class;
    }
}
