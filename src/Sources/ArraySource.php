<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Sources;

use Illuminate\Support\Collection;
use Robel\DataPipeline\Contracts\SourceInterface;

final readonly class ArraySource implements SourceInterface
{
    private array $items;

    /**
     * @param  array<int|string, mixed>|Collection<int|string, mixed>  $items
     */
    public function __construct(array|Collection $items)
    {
        $this->items = $items instanceof Collection ? $items->all() : $items;
    }

    public function records(): iterable
    {
        foreach ($this->items as $key => $item) {
            yield $key => (array) $item;
        }
    }

    public function count(): int
    {
        return count($this->items);
    }

    public function fingerprint(): string
    {
        return 'array:'.hash('xxh128', serialize($this->items));
    }
}
