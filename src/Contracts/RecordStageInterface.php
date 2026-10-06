<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Contracts;

use Robel\DataPipeline\RecordContext;

interface RecordStageInterface
{
    /**
     * Process an individual record context.
     */
    public function handleRecord(RecordContext $record): RecordContext;
}
