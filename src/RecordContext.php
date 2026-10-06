<?php

declare(strict_types=1);

namespace Robel\DataPipeline;

use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Database\Eloquent\Model;

final class RecordContext
{
    /**
     * @param  array<string, mixed>  $raw  Original raw input
     * @param  array<string, mixed>  $data  Normalized/transformed data
     * @param  int|string  $index  Sequence or row index
     * @param  string|null  $identifier  Optional business key (e.g. email, UUID)
     * @param  array<string, array<int, string>>  $errors  Keyed validation errors
     * @param  bool  $isDropped  Whether the record was filtered out
     * @param  array<string, mixed>  $metadata  Arbitrary stage metadata
     */
    public function __construct(
        private readonly array $raw,
        private array $data,
        private readonly int|string $index,
        private ?string $identifier = null,
        private array $errors = [],
        private bool $isDropped = false,
        private array $metadata = [],
        private readonly bool $isDryRun = false,
        private readonly bool $isPreview = false,
    ) {}

    public static function fromRaw(
        mixed $raw,
        int|string $index,
        ?string $identifier = null,
        bool $isDryRun = false,
        bool $isPreview = false,
    ): self {
        $normalized = match (true) {
            $raw instanceof Model => $raw->attributesToArray(),
            $raw instanceof Arrayable => $raw->toArray(),
            is_object($raw) => (array) $raw,
            is_array($raw) => $raw,
            default => ['value' => $raw],
        };

        return new self(
            raw: $normalized,
            data: $normalized,
            index: $index,
            identifier: $identifier,
            isDryRun: $isDryRun,
            isPreview: $isPreview,
        );
    }

    public function raw(): array
    {
        return $this->raw;
    }

    public function toArray(): array
    {
        return $this->data;
    }

    public function data(): array
    {
        return $this->data;
    }

    public function setData(array $data): self
    {
        $this->data = $data;

        return $this;
    }

    public function get(string $key, mixed $default = null): mixed
    {
        return data_get($this->data, $key, $default);
    }

    public function set(string $key, mixed $value): self
    {
        data_set($this->data, $key, $value);

        return $this;
    }

    public function index(): int|string
    {
        return $this->index;
    }

    public function identifier(): ?string
    {
        return $this->identifier;
    }

    public function setIdentifier(?string $identifier): self
    {
        $this->identifier = $identifier;

        return $this;
    }

    /**
     * Add a field-level error.
     */
    public function addError(string $field, string $message): self
    {
        $this->errors[$field][] = $message;

        return $this;
    }

    /**
     * Set multiple errors (e.g. from Laravel Validator::messages()->toArray()).
     *
     * @param  array<string, array<int, string>>  $errors
     */
    public function setErrors(array $errors): self
    {
        $this->errors = $errors;

        return $this;
    }

    /**
     * @return array<string, array<int, string>>
     */
    public function errors(): array
    {
        return $this->errors;
    }

    public function isValid(): bool
    {
        return empty($this->errors);
    }

    public function isInvalid(): bool
    {
        return ! empty($this->errors);
    }

    public function drop(): self
    {
        $this->isDropped = true;

        return $this;
    }

    public function isDropped(): bool
    {
        return $this->isDropped;
    }

    public function setMeta(string $key, mixed $value): self
    {
        $this->metadata[$key] = $value;

        return $this;
    }

    public function getMeta(string $key, mixed $default = null): mixed
    {
        return $this->metadata[$key] ?? $default;
    }

    public function isDryRun(): bool
    {
        return $this->isDryRun;
    }

    public function isPreview(): bool
    {
        return $this->isPreview;
    }
}
