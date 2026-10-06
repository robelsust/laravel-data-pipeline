<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\RecordContext;

final readonly class MapFieldsStage implements RecordStageInterface
{
    /**
     * @param  array<string, string>  $mapping  Maps source keys to target keys ['Source Col' => 'target_col']
     */
    public function __construct(
        private array $mapping,
    ) {}

    public function handleRecord(RecordContext $record): RecordContext
    {
        $data = $record->data();
        $mapped = [];

        foreach ($data as $key => $value) {
            $targetKey = $this->mapping[$key] ?? $key;
            $mapped[$targetKey] = $value;
        }

        return $record->setData($mapped);
    }
}
