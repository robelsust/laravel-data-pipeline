<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Models;

use Illuminate\Database\Eloquent\Concerns\HasUuids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Support\Carbon;

/**
 * @property string $id
 * @property string|null $name
 * @property string $status
 * @property string $source_type
 * @property string|null $source_fingerprint
 * @property array<string, mixed> $metadata
 * @property int|null $total_records
 * @property int $processed_records
 * @property int $successful_records
 * @property int $failed_records
 * @property Carbon|null $started_at
 * @property Carbon|null $completed_at
 */
class PipelineRun extends Model
{
    use HasUuids;

    protected $table = 'pipeline_runs';

    protected $guarded = [];

    public function getConnectionName(): ?string
    {
        return $this->connection ?? config('data-pipeline.connection');
    }

    protected function casts(): array
    {
        return [
            'metadata' => 'array',
            'total_records' => 'integer',
            'processed_records' => 'integer',
            'successful_records' => 'integer',
            'failed_records' => 'integer',
            'started_at' => 'datetime',
            'completed_at' => 'datetime',
        ];
    }

    /** @return HasMany<PipelineChunk, $this> */
    public function chunks(): HasMany
    {
        return $this->hasMany(PipelineChunk::class, 'pipeline_id');
    }

    /** @return HasMany<PipelineError, $this> */
    public function errors(): HasMany
    {
        return $this->hasMany(PipelineError::class, 'pipeline_id');
    }

    public function progressPercentage(): int
    {
        if ($this->total_records === null || $this->total_records === 0) {
            return $this->status === 'COMPLETED' ? 100 : 0;
        }

        return (int) round(($this->processed_records / $this->total_records) * 100);
    }
}
