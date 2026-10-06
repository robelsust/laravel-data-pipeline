<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Results;

use Carbon\CarbonImmutable;

final readonly class RecordFailure
{
    /**
     * @param  int|string  $index  The row or sequence index in the source
     * @param  string|null  $identifier  An optional business identifier (e.g. email, SKU)
     * @param  array<string, mixed>  $rawInput  Snapshot of the input record
     * @param  array<string, array<int, string>>  $errors  Keyed validation error messages
     * @param  string|null  $exception  Stack trace or message if a runtime exception occurred
     * @param  CarbonImmutable  $failedAt  Timestamp when failure was recorded
     */
    public function __construct(
        public int|string $index,
        public ?string $identifier,
        public array $rawInput,
        public array $errors,
        public ?string $exception = null,
        public CarbonImmutable $failedAt = new CarbonImmutable,
    ) {}

    public function index(): int|string
    {
        return $this->index;
    }

    public function identifier(): ?string
    {
        return $this->identifier;
    }

    public function rawInput(): array
    {
        return $this->rawInput;
    }

    public function errors(): array
    {
        return $this->errors;
    }

    /**
     * Flattened array of all error messages.
     *
     * @return array<int, string>
     */
    public function messages(): array
    {
        $messages = [];
        foreach ($this->errors as $fieldErrors) {
            foreach ($fieldErrors as $msg) {
                $messages[] = $msg;
            }
        }

        return $messages;
    }

    public function exception(): ?string
    {
        return $this->exception;
    }
}
