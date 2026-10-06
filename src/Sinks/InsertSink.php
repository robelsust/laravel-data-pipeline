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
 * High-throughput bulk database insert sink via QueryBuilder (DB::table).
 *
 * Respects the model's configured database connection and populates timestamps
 * if the model uses timestamps. Eloquent model events and mutators are bypassed for performance;
 * use ModelSink when per-model lifecycle events, mutators, or custom casts are required.
 */
final readonly class InsertSink implements SinkInterface
{
    private string $table;

    private ?string $connection;

    private bool $usesTimestamps;

    private ?string $createdAtColumn;

    private ?string $updatedAtColumn;

    /**
     * @param  class-string<Model>|string  $target  Model class name, table name, or connection.table
     * @param  string|null  $connection  Explicit database connection name
     */
    public function __construct(string $target, ?string $connection = null)
    {
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

        try {
            $query = $this->connection !== null
                ? DB::connection($this->connection)->table($this->table)
                : DB::table($this->table);

            $query->getConnection()->transaction(fn (): bool => $query->insert($payloads));

            return SinkResult::success(count($payloads));
        } catch (Throwable $e) {
            // Flag records in chunk with database error
            foreach ($chunk->valid() as $record) {
                $record->addError('_db', PersistenceError::describe($e));
            }

            return SinkResult::failure(count($payloads), ['exception' => PersistenceError::describe($e)]);
        }
    }
}
