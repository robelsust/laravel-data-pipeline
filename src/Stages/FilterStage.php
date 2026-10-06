<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Closure;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\RecordContext;

final class FilterStage implements RecordStageInterface
{
    private Closure $predicate;

    /**
     * @param  Closure(array, RecordContext): bool  $predicate
     */
    public function __construct(
        Closure $predicate,
    ) {
        $this->predicate = $predicate;
    }

    public function __serialize(): array
    {
        return [
            'predicate' => new SerializableClosure($this->predicate),
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->predicate = $data['predicate'] instanceof SerializableClosure
            ? $data['predicate']->getClosure()
            : $data['predicate'];
    }

    public function handleRecord(RecordContext $record): RecordContext
    {
        $keep = ($this->predicate)($record->data(), $record);

        if (! $keep) {
            $record->drop();
        }

        return $record;
    }
}
