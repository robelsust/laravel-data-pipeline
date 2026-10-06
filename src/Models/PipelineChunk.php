<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Support\Carbon;

/**
 * @property int $id
 * @property string $pipeline_id
 * @property int $sequence
 * @property string $status
 * @property int $records_count
 * @property int $successful_count
 * @property int $failed_count
 * @property int|null $last_record_offset
 * @property string|null $lease_token
 * @property Carbon|null $started_at
 * @property Carbon|null $completed_at
 */
class PipelineChunk extends Model
{
    public $timestamps = false;

    protected $table = 'pipeline_chunks';

    protected $guarded = [];

    public function getConnectionName(): ?string
    {
        return $this->connection ?? config('data-pipeline.connection');
    }

    protected function casts(): array
    {
        return [
            'sequence' => 'integer',
            'records_count' => 'integer',
            'successful_count' => 'integer',
            'failed_count' => 'integer',
            'last_record_offset' => 'integer',
            'started_at' => 'datetime',
            'completed_at' => 'datetime',
        ];
    }

    /** @return BelongsTo<PipelineRun, $this> */
    public function run(): BelongsTo
    {
        return $this->belongsTo(PipelineRun::class, 'pipeline_id');
    }

    /** @return HasMany<PipelineError, $this> */
    public function errors(): HasMany
    {
        return $this->hasMany(PipelineError::class, 'chunk_id');
    }
}
