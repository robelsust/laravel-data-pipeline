<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Models;

use Illuminate\Database\Eloquent\Concerns\HasUuids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class PipelineRun extends Model
{
    use HasUuids;

    protected $table = 'pipeline_runs';

    protected $guarded = [];

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

    public function chunks(): HasMany
    {
        return $this->hasMany(PipelineChunk::class, 'pipeline_id');
    }

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
