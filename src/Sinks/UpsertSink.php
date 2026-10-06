<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Sinks;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\Exceptions\PersistenceError;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\Results\SinkResult;
use Throwable;

/**
 * High-throughput bulk database upsert sink via QueryBuilder (DB::table).
 *
 * Respects the model's configured database connection and populates timestamps
 * if the model uses timestamps. Eloquent model events and mutators are bypassed for performance;
 * use ModelSink when per-model lifecycle events, mutators, or custom casts are required.
 */
final readonly class UpsertSink implements SinkInterface
{
    private string $table;

    private ?string $connection;

    private array $uniqueBy;

    private ?array $update;

    private bool $usesTimestamps;

    private ?string $createdAtColumn;

    private ?string $updatedAtColumn;

    /**
     * @param  class-string<Model>|string  $target  Model class name, table name, or connection.table
     * @param  array<int, string>|string  $uniqueBy  Unique column(s) for conflict resolution
     * @param  array<int, string>|null  $update  Columns to update on duplicate
     * @param  string|null  $connection  Explicit database connection name
     */
    public function __construct(
        string $target,
        array|string $uniqueBy,
        ?array $update = null,
        ?string $connection = null,
    ) {
        $resolvedConnection = $connection;
        $resolvedUsesTimestamps = false;
        $resolvedCreatedAt = null;
        $resolvedUpdatedAt = null;

        if (is_subclass_of($target, Model::class)) {
            $instance = new $target;
            $this->table = $instance->getTable();
            $resolvedConnection ??= $instance->getConnectionName();
            $resolvedUsesTimestamps = $instance->usesTimestamps();
            $resolvedCreatedAt = $instance->getCreatedAtColumn();
            $resolvedUpdatedAt = $instance->getUpdatedAtColumn();
        } elseif (str_contains($target, '.')) {
            [$connPrefix, $cleanTable] = explode('.', $target, 2);
            if (config("database.connections.{$connPrefix}") !== null) {
                $resolvedConnection ??= $connPrefix;
                $this->table = $cleanTable;
            } else {
                $this->table = $target;
            }
        } else {
            $this->table = $target;
        }

        $this->connection = $resolvedConnection;
        $this->uniqueBy = (array) $uniqueBy;
        $this->update = $update;
        $this->usesTimestamps = $resolvedUsesTimestamps;
        $this->createdAtColumn = $resolvedCreatedAt;
        $this->updatedAtColumn = $resolvedUpdatedAt;
    }

    public function getConnection(): ?string
    {
        return $this->connection;
    }

    public function persist(RecordChunk $chunk): SinkResult
    {
        $payloads = $chunk->validPayloads();

        if (empty($payloads)) {
            return SinkResult::success(0);
        }

        if ($this->usesTimestamps) {
            $now = now();
            foreach ($payloads as &$payload) {
                if ($this->createdAtColumn && ! isset($payload[$this->createdAtColumn])) {
                    $payload[$this->createdAtColumn] = $now;
                }
                if ($this->updatedAtColumn && ! isset($payload[$this->updatedAtColumn])) {
                    $payload[$this->updatedAtColumn] = $now;
                }
            }
            unset($payload);
        }

        // If update columns not specified, default to all non-unique columns from first record
        $updateColumns = $this->update;
        if ($updateColumns === null) {
            $allColumns = array_keys($payloads[0]);
            $updateColumns = array_values(array_diff($allColumns, $this->uniqueBy));
        }

        try {
            $query = $this->connection !== null
                ? DB::connection($this->connection)->table($this->table)
                : DB::table($this->table);

            $query->getConnection()->transaction(fn (): int => $query->upsert($payloads, $this->uniqueBy, $updateColumns));

            return SinkResult::success(count($payloads));
        } catch (Throwable $e) {
            foreach ($chunk->valid() as $record) {
                $record->addError('_db', PersistenceError::describe($e));
            }

            return SinkResult::failure(count($payloads), ['exception' => PersistenceError::describe($e)]);
        }
    }
}
