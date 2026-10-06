<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Contracts;

interface SourceInterface
{
    /**
     * Return an iterable stream of raw records.
     * Must maintain an O(1) memory profile regardless of dataset size.
     *
     * @return iterable<int|string, array<string, mixed>>
     */
    public function records(): iterable;

    /**
     * Estimated count of records, or null if streaming an unbounded/unknown source.
     */
    public function count(): ?int;

    /**
     * Return a stable identifier/fingerprint representing the source.
     */
    public function fingerprint(): string;
}
