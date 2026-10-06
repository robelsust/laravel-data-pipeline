<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Sinks;

use Closure;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\Results\SinkResult;

final class CustomSink implements SinkInterface
{
    private Closure $callback;

    /**
     * @param  Closure(RecordChunk): mixed  $callback
     */
    public function __construct(
        Closure $callback,
    ) {
        $this->callback = $callback;
    }

    public function __serialize(): array
    {
        return [
            'callback' => new SerializableClosure($this->callback),
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->callback = $data['callback'] instanceof SerializableClosure
            ? $data['callback']->getClosure()
            : $data['callback'];
    }

    public function persist(RecordChunk $chunk): SinkResult
    {
        $validRecords = $chunk->valid();
        $count = count($validRecords);

        if ($count === 0) {
            return SinkResult::success(0);
        }

        $result = ($this->callback)($chunk);

        if ($result instanceof SinkResult) {
            return $result;
        }

        return SinkResult::success($count);
    }
}
