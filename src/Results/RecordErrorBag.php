<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Results;

use ArrayIterator;
use Countable;
use IteratorAggregate;
use Traversable;

final class RecordErrorBag implements Countable, IteratorAggregate
{
    /**
     * @var array<int|string, RecordFailure>
     */
    private array $failures = [];

    private int $totalCount = 0;

    /**
     * @param  array<int|string, RecordFailure>  $failures
     */
    public function __construct(
        array $failures = [],
        private readonly int $maxCapacity = 5000,
    ) {
        foreach ($failures as $failure) {
            $this->add($failure);
        }
    }

    public function add(RecordFailure $failure): self
    {
        $this->totalCount++;

        if (count($this->failures) < $this->maxCapacity) {
            $this->failures[$failure->index()] = $failure;
        }

        return $this;
    }

    /**
     * Retrieve errors for a specific row index or identifier.
     */
    public function for(int|string $index): ?RecordFailure
    {
        return $this->failures[$index] ?? null;
    }

    /**
     * @return array<int|string, RecordFailure>
     */
    public function all(): array
    {
        return $this->failures;
    }

    public function first(): ?RecordFailure
    {
        $first = reset($this->failures);

        return $first !== false ? $first : null;
    }

    public function hasErrors(): bool
    {
        return $this->totalCount > 0;
    }

    public function isEmpty(): bool
    {
        return $this->totalCount === 0;
    }

    public function count(): int
    {
        return $this->totalCount;
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->failures);
    }
}
