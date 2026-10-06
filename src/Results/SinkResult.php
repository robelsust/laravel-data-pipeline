<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Results;

final readonly class SinkResult
{
    /**
     * @param  array<string, mixed>  $metadata
     * @param  array<int, int|string>  $persistedIndices
     * @param  array<int, int|string>  $failedIndices
     */
    public function __construct(
        public int $persistedCount = 0,
        public int $failedCount = 0,
        public array $metadata = [],
        public array $persistedIndices = [],
        public array $failedIndices = [],
    ) {}

    /**
     * @param  array<string, mixed>  $metadata
     * @param  array<int, int|string>  $persistedIndices
     */
    public static function success(int $count = 0, array $metadata = [], array $persistedIndices = []): self
    {
        return new self(
            persistedCount: $count,
            failedCount: 0,
            metadata: $metadata,
            persistedIndices: $persistedIndices,
        );
    }

    /**
     * @param  array<string, mixed>  $metadata
     * @param  array<int, int|string>  $failedIndices
     */
    public static function failure(int $count = 0, array $metadata = [], array $failedIndices = []): self
    {
        return new self(
            persistedCount: 0,
            failedCount: $count,
            metadata: $metadata,
            failedIndices: $failedIndices,
        );
    }

    /**
     * @param  array<string, mixed>  $metadata
     * @param  array<int, int|string>  $persistedIndices
     * @param  array<int, int|string>  $failedIndices
     */
    public static function partial(
        int $persisted,
        int $failed,
        array $metadata = [],
        array $persistedIndices = [],
        array $failedIndices = [],
    ): self {
        return new self(
            persistedCount: $persisted,
            failedCount: $failed,
            metadata: $metadata,
            persistedIndices: $persistedIndices,
            failedIndices: $failedIndices,
        );
    }
}
