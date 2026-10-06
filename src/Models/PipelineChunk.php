<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;

class PipelineChunk extends Model
{
    public $timestamps = false;

    protected $table = 'pipeline_chunks';

    protected $guarded = [];

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

    public function run(): BelongsTo
    {
        return $this->belongsTo(PipelineRun::class, 'pipeline_id');
    }

    public function errors(): HasMany
    {
        return $this->hasMany(PipelineError::class, 'chunk_id');
    }
}
