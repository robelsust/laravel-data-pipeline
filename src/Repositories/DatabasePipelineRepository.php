<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Repositories;

use Illuminate\Support\Facades\DB;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\PipelineStatus;

final class DatabasePipelineRepository implements PipelineRepositoryInterface
{
    public function createRun(
        string $sourceType,
        ?int $totalRecords = null,
        ?string $fingerprint = null,
        ?string $name = null,
        array $metadata = [],
    ): PipelineRun {
        return PipelineRun::create([
            'name' => $name,
            'status' => PipelineStatus::Created->value,
            'source_type' => $sourceType,
            'source_fingerprint' => $fingerprint,
            'total_records' => $totalRecords,
            'metadata' => $metadata,
            'started_at' => now(),
        ]);
    }

    public function updateRunProgress(
        string $runId,
        int $processedIncrement,
        int $successfulIncrement,
        int $failedIncrement
    ): void {
        DB::connection(config('data-pipeline.connection'))->table('pipeline_runs')
            ->where('id', $runId)
            ->update([
                'status' => PipelineStatus::Running->value,
                'processed_records' => DB::raw("processed_records + {$processedIncrement}"),
                'successful_records' => DB::raw("successful_records + {$successfulIncrement}"),
                'failed_records' => DB::raw("failed_records + {$failedIncrement}"),
                'updated_at' => now(),
            ]);
    }

    public function markRunCompleted(string $runId, string $status = PipelineStatus::Completed->value): void
    {
        DB::connection(config('data-pipeline.connection'))->table('pipeline_runs')
            ->where('id', $runId)
            ->update([
                'status' => $status,
                'completed_at' => now(),
                'updated_at' => now(),
            ]);
    }

    public function markRunFailed(string $runId, string $errorMessage): void
    {
        $run = PipelineRun::find($runId);
        $metadata = $run->metadata ?? [];
        $metadata['last_error'] = $errorMessage;

        DB::connection(config('data-pipeline.connection'))->table('pipeline_runs')
            ->where('id', $runId)
            ->update([
                'status' => PipelineStatus::Failed->value,
                'metadata' => json_encode($metadata),
                'completed_at' => now(),
                'updated_at' => now(),
            ]);
    }

    public function recordChunk(
        string $runId,
        int $sequence,
        string $status,
        int $recordsCount,
        int $successfulCount,
        int $failedCount,
        ?int $lastOffset = null,
        ?string $lastIdentifier = null,
    ): PipelineChunk {
        return PipelineChunk::updateOrCreate(
            ['pipeline_id' => $runId, 'sequence' => $sequence],
            [
                'status' => $status,
                'records_count' => $recordsCount,
                'successful_count' => $successfulCount,
                'failed_count' => $failedCount,
                'last_record_offset' => $lastOffset,
                'last_record_identifier' => $lastIdentifier,
                'completed_at' => now(),
            ]
        );
    }

    public function quarantineErrors(
        string $runId,
        ?int $chunkId,
        array $failures,
        array $redactedAttributes = [],
    ): void {
        if ($chunkId !== null) {
            $this->reconcileChunkErrors($runId, $chunkId, $failures, $redactedAttributes);

            return;
        }

        if (empty($failures)) {
            return;
        }

        $records = [];
        $now = now()->toDateTimeString();

        foreach ($failures as $failure) {
            $raw = $failure->rawInput();

            // Redact sensitive attributes if requested
            if (! empty($redactedAttributes)) {
                foreach ($redactedAttributes as $redactField) {
                    if (array_key_exists($redactField, $raw)) {
                        $raw[$redactField] = '[REDACTED]';
                    }
                }
            }

            $records[] = [
                'pipeline_id' => $runId,
                'chunk_id' => $chunkId,
                'record_index' => is_numeric($failure->index()) ? (int) $failure->index() : 0,
                'record_identifier' => $failure->identifier(),
                'raw_payload' => json_encode($raw, JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR),
                'validation_errors' => json_encode($failure->errors(), JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR),
                'exception_message' => $failure->exception(),
                'failed_at' => $now,
            ];
        }

        DB::connection(config('data-pipeline.connection'))->table('pipeline_errors')->insert($records);
    }

    public function reconcileChunkErrors(
        string $runId,
        int $chunkId,
        array $failures,
        array $redactedAttributes = [],
    ): void {
        $existingErrors = DB::connection(config('data-pipeline.connection'))->table('pipeline_errors')
            ->where('pipeline_id', $runId)
            ->where('chunk_id', $chunkId)
            ->get();

        $currentFailuresByKey = [];
        $currentFailuresByIndex = [];
        foreach ($failures as $failure) {
            $idx = is_numeric($failure->index()) ? (int) $failure->index() : 0;
            $currentFailuresByIndex[$idx] = $failure;
            if ($failure->identifier() !== null && $failure->identifier() !== '') {
                $currentFailuresByKey['id:'.$failure->identifier()] = $failure;
            }
            $currentFailuresByKey['idx:'.$idx] = $failure;
        }

        $preservedExistingIds = [];

        foreach ($existingErrors as $existing) {
            $isStillFailing = false;
            $matchedFailure = null;

            if ($existing->record_identifier !== null && isset($currentFailuresByKey['id:'.$existing->record_identifier])) {
                $isStillFailing = true;
                $matchedFailure = $currentFailuresByKey['id:'.$existing->record_identifier];
            } elseif (isset($currentFailuresByIndex[(int) $existing->record_index])) {
                $isStillFailing = true;
                $matchedFailure = $currentFailuresByIndex[(int) $existing->record_index];
            }

            if ($matchedFailure !== null) {
                $preservedExistingIds[] = $existing->id;

                $updateData = [];
                $raw = $matchedFailure->rawInput();
                if (! empty($redactedAttributes)) {
                    foreach ($redactedAttributes as $redactField) {
                        if (array_key_exists($redactField, $raw)) {
                            $raw[$redactField] = '[REDACTED]';
                        }
                    }
                }
                $updateData['raw_payload'] = json_encode($raw, JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR);
                $updateData['validation_errors'] = json_encode($matchedFailure->errors(), JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR);
                if ($matchedFailure->exception() !== null) {
                    $updateData['exception_message'] = $matchedFailure->exception();
                }
                DB::connection(config('data-pipeline.connection'))->table('pipeline_errors')
                    ->where('id', $existing->id)
                    ->update($updateData);
            } else {
                DB::connection(config('data-pipeline.connection'))->table('pipeline_errors')
                    ->where('id', $existing->id)
                    ->delete();
            }
        }

        $newRecords = [];
        $now = now()->toDateTimeString();

        foreach ($failures as $failure) {
            $idx = is_numeric($failure->index()) ? (int) $failure->index() : 0;
            $ident = $failure->identifier();

            $alreadyPreserved = false;
            foreach ($existingErrors as $existing) {
                if (in_array($existing->id, $preservedExistingIds, true)) {
                    if ($ident !== null && $existing->record_identifier === $ident) {
                        $alreadyPreserved = true;
                        break;
                    }
                    if ((int) $existing->record_index === $idx) {
                        $alreadyPreserved = true;
                        break;
                    }
                }
            }

            if (! $alreadyPreserved) {
                $raw = $failure->rawInput();
                if (! empty($redactedAttributes)) {
                    foreach ($redactedAttributes as $redactField) {
                        if (array_key_exists($redactField, $raw)) {
                            $raw[$redactField] = '[REDACTED]';
                        }
                    }
                }

                $newRecords[] = [
                    'pipeline_id' => $runId,
                    'chunk_id' => $chunkId,
                    'record_index' => $idx,
                    'record_identifier' => $ident,
                    'raw_payload' => json_encode($raw, JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR),
                    'validation_errors' => json_encode($failure->errors(), JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR),
                    'exception_message' => $failure->exception(),
                    'failed_at' => $now,
                ];
            }
        }

        if (! empty($newRecords)) {
            DB::connection(config('data-pipeline.connection'))->table('pipeline_errors')->insert($newRecords);
        }
    }

    public function findRun(string $runId): ?PipelineRun
    {
        return PipelineRun::with(['chunks', 'errors'])->find($runId);
    }

    public function getQuarantinedRecords(string $runId): array
    {
        $errors = PipelineError::where('pipeline_id', $runId)
            ->orderBy('record_index')
            ->get();

        $rows = [];
        foreach ($errors as $err) {
            $rows[$err->record_index] = $err->raw_payload;
        }

        return $rows;
    }
}
