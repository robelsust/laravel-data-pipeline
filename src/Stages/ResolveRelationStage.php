<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Closure;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\Contracts\ChunkStageInterface;
use Robel\DataPipeline\RecordChunk;

final class ResolveRelationStage implements ChunkStageInterface
{
    private string $foreignKey;

    private string $lookupColumn;

    private string $table;

    private string $targetKey;

    private string $primaryKey;

    private bool $required;

    private ?Closure $autoCreate;

    /**
     * @param  string  $foreignKey  The column to set on the record (e.g. 'category_id')
     * @param  string  $lookupColumn  The column in the record holding the lookup value (e.g. 'category_name')
     * @param  class-string<Model>|string  $target  Target Model class or table name
     * @param  string  $targetKey  The column on the target table to match against (e.g. 'name' or 'slug')
     * @param  string  $primaryKey  The column on the target table to retrieve (e.g. 'id')
     * @param  bool  $required  If true, records with unresolved values receive a validation error
     * @param  (Closure(string): (int|string|null))|null  $autoCreate  Optional closure to create missing relations on the fly
     */
    public function __construct(
        string $foreignKey,
        string $lookupColumn,
        string $target,
        string $targetKey = 'name',
        string $primaryKey = 'id',
        bool $required = true,
        ?Closure $autoCreate = null,
    ) {
        $this->foreignKey = $foreignKey;
        $this->lookupColumn = $lookupColumn;
        $this->targetKey = $targetKey;
        $this->primaryKey = $primaryKey;
        $this->required = $required;
        $this->autoCreate = $autoCreate;

        if (is_subclass_of($target, Model::class)) {
            $this->table = (new $target)->getTable();
        } else {
            $this->table = $target;
        }
    }

    public function __serialize(): array
    {
        return [
            'foreignKey' => $this->foreignKey,
            'lookupColumn' => $this->lookupColumn,
            'table' => $this->table,
            'targetKey' => $this->targetKey,
            'primaryKey' => $this->primaryKey,
            'required' => $this->required,
            'autoCreate' => $this->autoCreate !== null ? new SerializableClosure($this->autoCreate) : null,
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->foreignKey = $data['foreignKey'];
        $this->lookupColumn = $data['lookupColumn'];
        $this->table = $data['table'];
        $this->targetKey = $data['targetKey'];
        $this->primaryKey = $data['primaryKey'];
        $this->required = $data['required'];
        $this->autoCreate = isset($data['autoCreate']) && $data['autoCreate'] instanceof SerializableClosure
            ? $data['autoCreate']->getClosure()
            : null;
    }

    public function handleChunk(RecordChunk $chunk): RecordChunk
    {
        // 1. Gather all unique lookup values in the chunk
        $lookupValues = [];
        foreach ($chunk->records() as $record) {
            if ($record->isDropped()) {
                continue;
            }

            $val = $record->get($this->lookupColumn);
            if ($val !== null && $val !== '') {
                $lookupValues[] = (string) $val;
            }
        }

        $uniqueLookups = array_unique($lookupValues);
        $resolvedMap = [];

        if (! empty($uniqueLookups)) {
            // 2. Fetch existing relations in a single WHERE IN query (O(1) database queries per chunk)
            $resolvedMap = DB::table($this->table)
                ->whereIn($this->targetKey, $uniqueLookups)
                ->pluck($this->primaryKey, $this->targetKey)
                ->all();

            // 3. Auto-create missing entries if autoCreate closure provided
            if ($this->autoCreate !== null) {
                foreach ($uniqueLookups as $val) {
                    if (! isset($resolvedMap[$val])) {
                        if ($chunk->isDryRun() || $chunk->isPreview()) {
                            $resolvedMap[$val] = "simulated-{$val}";
                        } else {
                            $res = ($this->autoCreate)($val);
                            $newId = match (true) {
                                $res instanceof Model => $res->getKey(),
                                is_array($res) => DB::table($this->table)->insertGetId($res),
                                default => $res,
                            };
                            if ($newId !== null) {
                                $resolvedMap[$val] = $newId;
                            }
                        }
                    }
                }
            }
        }

        // 4. Assign resolved foreign keys to each record
        foreach ($chunk->records() as $record) {
            if ($record->isDropped()) {
                continue;
            }

            $val = $record->get($this->lookupColumn);
            if ($val === null || $val === '') {
                if ($this->required) {
                    $record->addError($this->foreignKey, "Missing required lookup value for [{$this->lookupColumn}].");
                }

                continue;
            }

            $valStr = (string) $val;
            if (isset($resolvedMap[$valStr])) {
                $record->set($this->foreignKey, $resolvedMap[$valStr]);
            } elseif ($this->required) {
                $record->addError($this->foreignKey, "Could not resolve {$this->foreignKey} for '{$valStr}'.");
            }
        }

        return $chunk;
    }
}
