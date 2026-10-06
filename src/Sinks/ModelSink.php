<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Sinks;

use Illuminate\Database\Eloquent\Model;
use InvalidArgumentException;
use Robel\DataPipeline\Contracts\SinkInterface;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\Results\SinkResult;
use Throwable;

/**
 * Eloquent Model persistence sink.
 *
 * Persists records via individual Model instances, executing Eloquent model events
 * (creating, created, saving, saved), mutators, attribute casting, and relationships.
 */
final readonly class ModelSink implements SinkInterface
{
    /**
     * @param  class-string<Model>  $modelClass
     * @param  string|null  $connection  Optional explicit database connection name
     */
    public function __construct(
        private string $modelClass,
        private ?string $connection = null,
    ) {
        if (! is_subclass_of($modelClass, Model::class)) {
            throw new InvalidArgumentException("ModelSink expects an Eloquent Model class, got '{$modelClass}'.");
        }
    }

    public function getConnection(): ?string
    {
        return $this->connection ?? (new $this->modelClass)->getConnectionName();
    }

    public function persist(RecordChunk $chunk): SinkResult
    {
        $records = $chunk->valid();
        if (empty($records)) {
            return SinkResult::success(0);
        }

        $persisted = 0;
        $failed = 0;
        $persistedIndices = [];
        $failedIndices = [];

        foreach ($records as $record) {
            try {
                /** @var Model $model */
                $model = new $this->modelClass;
                if ($this->connection !== null) {
                    $model->setConnection($this->connection);
                }

                $model->fill($record->data());
                $saved = $model->save();

                if ($saved) {
                    $persisted++;
                    $persistedIndices[] = $record->index();
                } else {
                    $failed++;
                    $failedIndices[] = $record->index();
                    $record->addError('_model', 'Model save returned false.');
                }
            } catch (Throwable $e) {
                $failed++;
                $failedIndices[] = $record->index();
                $record->addError('_model', $e->getMessage());
            }
        }

        if ($failed > 0 && $persisted > 0) {
            return SinkResult::partial(
                persisted: $persisted,
                failed: $failed,
                metadata: ['persisted' => $persisted, 'failed' => $failed],
                persistedIndices: $persistedIndices,
                failedIndices: $failedIndices
            );
        }

        if ($failed > 0) {
            return SinkResult::failure(
                count: $failed,
                metadata: ['persisted' => 0, 'failed' => $failed],
                failedIndices: $failedIndices
            );
        }

        return SinkResult::success(
            count: $persisted,
            metadata: ['persisted' => $persisted, 'failed' => 0],
            persistedIndices: $persistedIndices
        );
    }
}
