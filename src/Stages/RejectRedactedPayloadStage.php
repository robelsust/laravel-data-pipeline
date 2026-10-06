<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Illuminate\Support\Arr;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\RecordContext;

/** Requires retry definitions to restore masked values before persistence. */
final class RejectRedactedPayloadStage implements RecordStageInterface
{
    public function handleRecord(RecordContext $record): RecordContext
    {
        foreach (Arr::dot($record->data()) as $field => $value) {
            if ($value === '[REDACTED]') {
                $record->addError((string) $field, 'Restore the redacted value in the retry definition before persisting this record.');
            }
        }

        return $record;
    }
}
