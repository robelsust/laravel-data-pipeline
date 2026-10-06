<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property int $id
 * @property string $pipeline_id
 * @property int|null $chunk_id
 * @property int $record_index
 * @property string|null $record_identifier
 * @property array<string, mixed>|null $raw_payload
 * @property array<string, array<int,string>> $validation_errors
 * @property string|null $exception_message
 * @property Carbon $failed_at
 */
class PipelineError extends Model
{
    public $timestamps = false;

    protected $table = 'pipeline_errors';

    protected $guarded = [];

    public function getConnectionName(): ?string
    {
        return $this->connection ?? config('data-pipeline.connection');
    }

    protected function casts(): array
    {
        return [
            'record_index' => 'integer',
            'raw_payload' => 'array',
            'validation_errors' => 'array',
            'failed_at' => 'datetime',
        ];
    }

    /** @return BelongsTo<PipelineRun, $this> */
    public function run(): BelongsTo
    {
        return $this->belongsTo(PipelineRun::class, 'pipeline_id');
    }

    /** @return BelongsTo<PipelineChunk, $this> */
    public function chunk(): BelongsTo
    {
        return $this->belongsTo(PipelineChunk::class, 'chunk_id');
    }
}
