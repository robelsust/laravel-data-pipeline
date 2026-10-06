<?php

declare(strict_types=1);

namespace Robel\DataPipeline;

use ArrayIterator;
use Countable;
use IteratorAggregate;
use Traversable;

final class RecordChunk implements Countable, IteratorAggregate
{
    /**
     * @param  array<int, RecordContext>  $records
     * @param  int  $sequence  The sequential number of this chunk (1, 2, 3...)
     */
    public function __construct(
        private array $records,
        private readonly int $sequence = 1,
        private readonly bool $isDryRun = false,
        private readonly bool $isPreview = false,
    ) {}

    public function isDryRun(): bool
    {
        return $this->isDryRun;
    }

    public function isPreview(): bool
    {
        return $this->isPreview;
    }

    /**
     * @return array<int, RecordContext>
     */
    public function records(): array
    {
        return $this->records;
    }

    public function sequence(): int
    {
        return $this->sequence;
    }

    /**
     * Valid and non-dropped records.
     *
     * @return array<int, RecordContext>
     */
    public function valid(): array
    {
        return array_values(array_filter(
            $this->records,
            fn (RecordContext $r) => $r->isValid() && ! $r->isDropped()
        ));
    }

    /**
     * Records with validation or processing errors.
     *
     * @return array<int, RecordContext>
     */
    public function invalid(): array
    {
        return array_values(array_filter(
            $this->records,
            fn (RecordContext $r) => $r->isInvalid()
        ));
    }

    /**
     * Records that were intentionally dropped by a filter stage.
     *
     * @return array<int, RecordContext>
     */
    public function dropped(): array
    {
        return array_values(array_filter(
            $this->records,
            fn (RecordContext $r) => $r->isDropped()
        ));
    }

    /**
     * Extract pure data arrays for all valid records (suitable for bulk DB insert/upsert).
     *
     * @return array<int, array<string, mixed>>
     */
    public function validPayloads(): array
    {
        return array_map(
            fn (RecordContext $r) => $r->toArray(),
            $this->valid()
        );
    }

    public function count(): int
    {
        return count($this->records);
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->records);
    }
}
