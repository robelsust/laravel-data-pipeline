<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class PipelineError extends Model
{
    public $timestamps = false;

    protected $table = 'pipeline_errors';

    protected $guarded = [];

    protected function casts(): array
    {
        return [
            'record_index' => 'integer',
            'raw_payload' => 'array',
            'validation_errors' => 'array',
            'failed_at' => 'datetime',
        ];
    }

    public function run(): BelongsTo
    {
        return $this->belongsTo(PipelineRun::class, 'pipeline_id');
    }

    public function chunk(): BelongsTo
    {
        return $this->belongsTo(PipelineChunk::class, 'chunk_id');
    }
}
