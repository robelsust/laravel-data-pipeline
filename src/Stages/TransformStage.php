<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Closure;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\RecordContext;

final class TransformStage implements RecordStageInterface
{
    private Closure|array $transformer;

    /**
     * @param  Closure|array<string, Closure>  $transformer
     */
    public function __construct(
        Closure|array $transformer,
    ) {
        $this->transformer = $transformer;
    }

    public function __serialize(): array
    {
        if ($this->transformer instanceof Closure) {
            return [
                'type' => 'closure',
                'transformer' => new SerializableClosure($this->transformer),
            ];
        }

        $transformers = [];
        foreach ($this->transformer as $field => $closure) {
            $transformers[$field] = $closure instanceof Closure ? new SerializableClosure($closure) : $closure;
        }

        return [
            'type' => 'array',
            'transformer' => $transformers,
        ];
    }

    public function __unserialize(array $data): void
    {
        if (($data['type'] ?? '') === 'closure') {
            $this->transformer = $data['transformer'] instanceof SerializableClosure
                ? $data['transformer']->getClosure()
                : $data['transformer'];
        } else {
            $this->transformer = [];
            foreach (($data['transformer'] ?? []) as $field => $closure) {
                $this->transformer[$field] = $closure instanceof SerializableClosure
                    ? $closure->getClosure()
                    : $closure;
            }
        }
    }

    public function handleRecord(RecordContext $record): RecordContext
    {
        if ($this->transformer instanceof Closure) {
            $result = ($this->transformer)($record->data(), $record);

            if ($result instanceof RecordContext) {
                return $result;
            }

            if (is_array($result)) {
                return $record->setData($result);
            }

            return $record;
        }

        // Field-level transformers: ['email' => fn($val) => strtolower($val)]
        $data = $record->data();
        foreach ($this->transformer as $field => $closure) {
            if (array_key_exists($field, $data)) {
                $data[$field] = $closure($data[$field], $data, $record);
            }
        }

        return $record->setData($data);
    }
}
