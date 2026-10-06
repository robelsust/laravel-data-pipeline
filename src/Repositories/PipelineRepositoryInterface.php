<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Repositories;

use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Results\RecordFailure;

interface PipelineRepositoryInterface
{
    public function createRun(
        string $sourceType,
        ?int $totalRecords = null,
        ?string $fingerprint = null,
        ?string $name = null,
        array $metadata = [],
    ): PipelineRun;

    public function updateRunProgress(
        string $runId,
        int $processedIncrement,
        int $successfulIncrement,
        int $failedIncrement
    ): void;

    public function markRunCompleted(string $runId, string $status = 'COMPLETED'): void;

    public function markRunFailed(string $runId, string $errorMessage): void;

    public function recordChunk(
        string $runId,
        int $sequence,
        string $status,
        int $recordsCount,
        int $successfulCount,
        int $failedCount,
        ?int $lastOffset = null,
        ?string $lastIdentifier = null,
    ): PipelineChunk;

    /**
     * @param  array<int, RecordFailure>  $failures
     * @param  array<int, string>  $redactedAttributes
     */
    public function quarantineErrors(
        string $runId,
        ?int $chunkId,
        array $failures,
        array $redactedAttributes = [],
    ): void;

    /**
     * Reconcile quarantine errors when replacing or updating a chunk checkpoint.
     * Deletes obsolete error entries for records that now succeed,
     * preserves existing entries for records that still fail,
     * and inserts new entries for newly failing records.
     *
     * @param  array<int, RecordFailure>  $failures
     * @param  array<int, string>  $redactedAttributes
     */
    public function reconcileChunkErrors(
        string $runId,
        int $chunkId,
        array $failures,
        array $redactedAttributes = [],
    ): void;

    public function findRun(string $runId): ?PipelineRun;

    /**
     * Fetch quarantined records for a given pipeline run (for surgical retry).
     *
     * @return array<int, array<string, mixed>>
     */
    public function getQuarantinedRecords(string $runId): array;
}
