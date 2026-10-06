<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Contracts;

use Robel\DataPipeline\DataPipeline;

interface PipelineDefinitionInterface
{
    public function pipeline(): DataPipeline;
}
