<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Validation;

use Closure;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\Rules\Exists;
use Illuminate\Validation\Rules\Unique;
use ReflectionClass;
use Robel\DataPipeline\RecordChunk;

final class BatchValidator
{
    /**
     * @param  array<string, mixed>  $rules
     * @param  array<string, string>  $messages
     * @param  array<string, string>  $customAttributes
     */
    public function __construct(
        private readonly array $rules,
        private readonly array $messages = [],
        private readonly array $customAttributes = [],
    ) {}

    /**
     * Validate all records in a chunk, batching database unique and exists checks to eliminate N+1 queries.
     */
    public function validateChunk(RecordChunk $chunk): void
    {
        // 1. Separate database rules (unique, exists) from in-memory standard rules
        [$standardRules, $uniqueRules, $existsRules] = $this->extractDatabaseRules($this->rules);

        $excludedByRecord = [];
        $databaseFields = array_unique(array_merge(array_keys($uniqueRules), array_keys($existsRules)));
        foreach ($chunk->records() as $record) {
            if ($record->isDropped() || $standardRules === []) {
                continue;
            }
            $validator = Validator::make($record->data(), $standardRules, $this->messages, $this->customAttributes);
            if ($validator->fails()) {
                $record->setErrors(array_merge($record->errors(), $validator->errors()->toArray()));
            }
            foreach ($databaseFields as $field) {
                if (! Arr::has($validator->getData(), $field)) {
                    $excludedByRecord[spl_object_id($record)][$field] = true;
                }
            }
        }

        $existingUniqueValues = $this->fetchExistingValues($chunk, $uniqueRules, isUnique: true, excludedByRecord: $excludedByRecord);
        $existingExistsValues = $this->fetchExistingValues($chunk, $existsRules, isUnique: false, excludedByRecord: $excludedByRecord);
        $seenInChunk = [];

        foreach ($chunk->records() as $record) {
            if ($record->isDropped()) {
                continue;
            }
            $isFieldExcluded = fn (string $field): bool => isset($excludedByRecord[spl_object_id($record)][$field]);

            // Evaluate batched uniqueness
            foreach ($uniqueRules as $field => $uniqueConfig) {
                if ($isFieldExcluded($field)) {
                    continue;
                }

                $value = $record->get($field);
                if ($value === null || $value === '') {
                    continue;
                }

                $values = is_array($value) ? $value : [$value];
                foreach ($values as $itemVal) {
                    if ($itemVal === null || $itemVal === '' || is_array($itemVal)) {
                        continue;
                    }

                    $valueStr = (string) $itemVal;
                    $attributeName = $this->customAttributes[$field] ?? str_replace('_', ' ', $field);

                    if (isset($existingUniqueValues[$field][$valueStr])) {
                        $msg = $this->messages["{$field}.unique"]
                            ?? $this->messages['unique']
                            ?? "The {$attributeName} has already been taken.";
                        $msg = str_replace(':attribute', $attributeName, $msg);
                        $record->addError($field, $msg);

                        break;
                    }

                    if (isset($seenInChunk[$field][$valueStr])) {
                        $msg = $this->messages["{$field}.distinct"]
                            ?? $this->messages["{$field}.duplicate"]
                            ?? $this->messages['distinct']
                            ?? "The {$attributeName} is duplicated within the current batch.";
                        $msg = str_replace(':attribute', $attributeName, $msg);
                        $record->addError($field, $msg);

                        break;
                    }

                    $seenInChunk[$field][$valueStr] = true;
                }
            }

            // Evaluate batched existence (eliminating N+1 exists queries)
            foreach ($existsRules as $field => $existsConfig) {
                if ($isFieldExcluded($field)) {
                    continue;
                }

                $value = $record->get($field);
                if ($value === null || $value === '') {
                    continue;
                }

                $values = is_array($value) ? $value : [$value];
                foreach ($values as $itemVal) {
                    if ($itemVal === null || $itemVal === '' || is_array($itemVal)) {
                        continue;
                    }

                    $valueStr = (string) $itemVal;

                    if (! isset($existingExistsValues[$field][$valueStr])) {
                        $attributeName = $this->customAttributes[$field] ?? str_replace('_', ' ', $field);
                        $msg = $this->messages["{$field}.exists"]
                            ?? $this->messages['exists']
                            ?? "The selected {$attributeName} is invalid.";
                        $msg = str_replace(':attribute', $attributeName, $msg);
                        $record->addError($field, $msg);

                        break;
                    }
                }
            }
        }
    }

    /**
     * @return array{0: array<string, mixed>, 1: array<string, array{table: string, column: string, ignore: mixed, idColumn: string, wheres: array}>, 2: array<string, array{table: string, column: string, wheres: array}>}
     */
    private function extractDatabaseRules(array $rules): array
    {
        $standard = [];
        $unique = [];
        $exists = [];

        foreach ($rules as $field => $fieldRules) {
            $rulesList = is_array($fieldRules) ? $fieldRules : (is_string($fieldRules) ? explode('|', $fieldRules) : [$fieldRules]);
            $keptRules = [];

            foreach ($rulesList as $rule) {
                if (is_string($rule) && str_starts_with($rule, 'unique:')) {
                    $params = explode(',', substr($rule, 7));
                    $table = $params[0];
                    $column = $params[1] ?? $field;
                    $ignore = isset($params[2]) && $params[2] !== 'NULL' && $params[2] !== '' ? $params[2] : null;
                    $idColumn = $params[3] ?? 'id';

                    $wheres = [];
                    for ($i = 4; $i < count($params); $i += 2) {
                        if (isset($params[$i + 1])) {
                            $wheres[] = ['column' => trim($params[$i]), 'value' => trim(trim($params[$i + 1]), '"\'')];
                        }
                    }

                    $unique[$field] = [
                        'table' => trim($table),
                        'column' => trim($column),
                        'ignore' => $ignore,
                        'idColumn' => trim($idColumn),
                        'wheres' => $wheres,
                    ];
                } elseif ($rule instanceof Unique) {
                    $ref = new ReflectionClass($rule);
                    $tableProp = $ref->hasProperty('table') ? $ref->getProperty('table') : null;
                    $colProp = $ref->hasProperty('column') ? $ref->getProperty('column') : null;
                    $ignoreProp = $ref->hasProperty('ignore') ? $ref->getProperty('ignore') : null;
                    $idColProp = $ref->hasProperty('idColumn') ? $ref->getProperty('idColumn') : null;
                    $wheresProp = $ref->hasProperty('wheres') ? $ref->getProperty('wheres') : null;
                    $usingProp = $ref->hasProperty('using') ? $ref->getProperty('using') : null;

                    $table = $tableProp ? $tableProp->getValue($rule) : '';
                    $col = $colProp ? $colProp->getValue($rule) : $field;

                    if ($col === 'NULL' || empty($col)) {
                        $col = $field;
                    }

                    if (empty($table)) {
                        throw new \InvalidArgumentException("Database Unique rule for field '{$field}' has an invalid or unresolvable table definition.");
                    }

                    $ignore = $ignoreProp ? $ignoreProp->getValue($rule) : null;
                    $idColumn = $idColProp ? $idColProp->getValue($rule) : 'id';
                    $wheres = $wheresProp ? $wheresProp->getValue($rule) : [];
                    $using = $usingProp ? $usingProp->getValue($rule) : [];

                    $unique[$field] = [
                        'table' => (string) $table,
                        'column' => (string) $col,
                        'ignore' => $ignore,
                        'idColumn' => (string) ($idColumn ?: 'id'),
                        'wheres' => $wheres,
                        'using' => $using,
                    ];
                } elseif (is_string($rule) && str_starts_with($rule, 'exists:')) {
                    $params = explode(',', substr($rule, 7));
                    $table = $params[0];
                    $column = $params[1] ?? $field;

                    $wheres = [];
                    for ($i = 2; $i < count($params); $i += 2) {
                        if (isset($params[$i + 1])) {
                            $wheres[] = ['column' => trim($params[$i]), 'value' => trim(trim($params[$i + 1]), '"\'')];
                        }
                    }

                    $exists[$field] = [
                        'table' => trim($table),
                        'column' => trim($column),
                        'wheres' => $wheres,
                        'using' => [],
                    ];
                } elseif ($rule instanceof Exists) {
                    $ref = new ReflectionClass($rule);
                    $tableProp = $ref->hasProperty('table') ? $ref->getProperty('table') : null;
                    $colProp = $ref->hasProperty('column') ? $ref->getProperty('column') : null;
                    $wheresProp = $ref->hasProperty('wheres') ? $ref->getProperty('wheres') : null;
                    $usingProp = $ref->hasProperty('using') ? $ref->getProperty('using') : null;

                    $table = $tableProp ? $tableProp->getValue($rule) : '';
                    $col = $colProp ? $colProp->getValue($rule) : $field;

                    if ($col === 'NULL' || empty($col)) {
                        $col = $field;
                    }

                    if (empty($table)) {
                        throw new \InvalidArgumentException("Database Exists rule for field '{$field}' has an invalid or unresolvable table definition.");
                    }

                    $wheres = $wheresProp ? $wheresProp->getValue($rule) : [];
                    $using = $usingProp ? $usingProp->getValue($rule) : [];

                    $exists[$field] = [
                        'table' => (string) $table,
                        'column' => (string) $col,
                        'wheres' => $wheres,
                        'using' => $using,
                    ];
                } else {
                    $keptRules[] = $rule;
                }
            }

            if (! empty($keptRules)) {
                $standard[$field] = $keptRules;
            }
        }

        return [$standard, $unique, $exists];
    }

    /**
     * @param  array<string, array{table: string, column: string, ignore?: mixed, idColumn?: string, wheres?: array, using?: array}>  $dbRules
     * @return array<string, array<string, bool>>
     */
    private function fetchExistingValues(RecordChunk $chunk, array $dbRules, bool $isUnique = false, array $excludedByRecord = []): array
    {
        $existing = [];

        foreach ($dbRules as $field => $config) {
            $table = $config['table'];
            $column = $config['column'];
            $existing[$field] = [];

            if (empty($table) || empty($column)) {
                continue;
            }

            $valuesInChunk = [];
            foreach ($chunk->records() as $record) {
                if ($record->isDropped() || isset($excludedByRecord[spl_object_id($record)][$field])) {
                    continue;
                }
                $val = $record->get($field);
                if ($val !== null && $val !== '') {
                    if (is_array($val)) {
                        foreach ($val as $subVal) {
                            if ($subVal !== null && $subVal !== '' && ! is_array($subVal)) {
                                $valuesInChunk[] = (string) $subVal;
                            }
                        }
                    } else {
                        $valuesInChunk[] = (string) $val;
                    }
                }
            }

            $uniqueValues = array_unique($valuesInChunk);
            if (empty($uniqueValues)) {
                continue;
            }

            $connection = null;
            if (str_contains($table, '.')) {
                [$connPrefix, $cleanTable] = explode('.', $table, 2);
                if (config("database.connections.{$connPrefix}") !== null) {
                    $connection = $connPrefix;
                    $table = $cleanTable;
                }
            }

            $query = $connection !== null
                ? DB::connection($connection)->table($table)->whereIn($column, $uniqueValues)
                : DB::table($table)->whereIn($column, $uniqueValues);

            // Apply ignore condition for unique rules
            if ($isUnique && isset($config['ignore'])) {
                $ignoreVal = $config['ignore'];
                $idCol = $config['idColumn'] ?? 'id';
                if ($ignoreVal instanceof Model) {
                    $idCol = $ignoreVal->getKeyName();
                    $ignoreVal = $ignoreVal->getKey();
                }
                $query->where($idCol, '!=', $ignoreVal);
            }

            // Apply scoped where conditions
            if (! empty($config['wheres'])) {
                foreach ($config['wheres'] as $where) {
                    if (is_array($where) && isset($where['column'])) {
                        $col = $where['column'];
                        $val = $where['value'] ?? null;
                        if (is_string($val) && strtoupper($val) === 'NULL') {
                            $query->whereNull($col);
                        } elseif (is_string($val) && strtoupper($val) === 'NOT_NULL') {
                            $query->whereNotNull($col);
                        } elseif (is_string($val) && str_starts_with($val, '!')) {
                            $query->where($col, '!=', substr($val, 1));
                        } else {
                            $query->where($col, $val);
                        }
                    } elseif ($where instanceof Closure) {
                        $query->where($where);
                    }
                }
            }

            // Apply using callbacks
            if (! empty($config['using'])) {
                foreach ($config['using'] as $callback) {
                    $callback($query);
                }
            }

            $found = $query->pluck($column)->all();

            foreach ($found as $val) {
                $existing[$field][(string) $val] = true;
            }
        }

        return $existing;
    }
}
