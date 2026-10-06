<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Results;

final readonly class PipelineResult
{
    public function __construct(
        public int $totalProcessed,
        public int $successful,
        public int $failed,
        public RecordErrorBag $errors,
        public float $durationInSeconds,
        public array $metadata = [],
    ) {}

    public function totalProcessed(): int
    {
        return $this->totalProcessed;
    }

    public function successful(): int
    {
        return $this->successful;
    }

    public function failed(): int
    {
        return $this->failed;
    }

    public function errors(): RecordErrorBag
    {
        return $this->errors;
    }

    public function duration(): float
    {
        return $this->durationInSeconds;
    }

    public function isSuccessful(): bool
    {
        return $this->failed === 0;
    }

    public function hasFailures(): bool
    {
        return $this->failed > 0;
    }
}
