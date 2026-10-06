<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Sources;

use Closure;
use Generator;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\LazyCollection;
use LogicException;
use Robel\DataPipeline\Contracts\SourceInterface;
use Traversable;

final class LazySource implements SourceInterface
{
    private bool $consumed = false;

    /**
     * @param  LazyCollection|Traversable|iterable|Closure  $source
     */
    public function __construct(
        private readonly mixed $source,
        private readonly ?int $estimatedCount = null,
        private readonly ?string $customFingerprint = null,
    ) {}

    public function isReplayable(): bool
    {
        return ! ($this->source instanceof Generator);
    }

    public function records(): iterable
    {
        $iterable = $this->source;

        if ($iterable instanceof Closure) {
            $iterable = $iterable();
        }

        if ($iterable instanceof Generator) {
            if ($this->consumed) {
                throw new LogicException(
                    'Direct Generator instances cannot be rewound or replayed across preview/dryRun/run. Wrap the generator in a Closure (e.g. fn() => yield ...) or LazyCollection::make() for replayable execution.'
                );
            }
            $this->consumed = true;
        }

        foreach ($iterable as $key => $item) {
            $record = match (true) {
                $item instanceof Model => $item->attributesToArray(),
                $item instanceof Arrayable => $item->toArray(),
                is_object($item) => (array) $item,
                is_array($item) => $item,
                default => ['value' => $item],
            };

            yield $key => $record;
        }
    }

    public function count(): ?int
    {
        if ($this->estimatedCount !== null) {
            return $this->estimatedCount;
        }

        if ($this->source instanceof LazyCollection) {
            // Do not force count if it would exhaust generator unless already known
            return null;
        }

        return null;
    }

    public function fingerprint(): string
    {
        return $this->customFingerprint ?? ('lazy:'.spl_object_hash((object) $this->source));
    }
}
