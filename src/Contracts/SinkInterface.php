<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Contracts;

use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\Results\SinkResult;

interface SinkInterface
{
    /**
     * Persist a chunk of validated, transformed records to the terminal destination.
     */
    public function persist(RecordChunk $chunk): SinkResult;
}
