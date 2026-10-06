<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Contracts;

use Robel\DataPipeline\RecordChunk;

interface ChunkStageInterface
{
    /**
     * Process an entire chunk of records at once.
     */
    public function handleChunk(RecordChunk $chunk): RecordChunk;
}
