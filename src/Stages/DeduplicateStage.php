<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Closure;
use Illuminate\Support\Facades\Cache;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\RecordContext;

final class DeduplicateStage implements RecordStageInterface
{
    /**
     * @var array<string, bool>
     */
    private array $seen = [];

    private string|array|Closure $key;

    private string $action;

    private ?string $store;

    private ?string $scope;

    private int $maxKeys;

    private bool $chunkLocal;

    /**
     * @var array<string, array{cache_key: ?string, hash: string, owner: string, index: int|string, identifier: ?string}>
     */
    private array $pendingClaims = [];

    /**
     * @param  string|array<int, string>|Closure(array<string, mixed>, RecordContext): string  $key
     * @param  string  $action  'drop' to discard duplicate records silently, or 'error' to flag validation error
     * @param  string|null  $store  Optional Cache store name for distributed deduplication across queue workers
     * @param  string|null  $scope  Optional namespace scope for distributed cache keys
     * @param  int  $maxKeys  Maximum in-memory signature entries retained before evicting oldest keys to maintain O(1) memory
     * @param  bool  $chunkLocal  Whether deduplication should only apply within each individual chunk
     */
    public function __construct(
        string|array|Closure $key,
        string $action = 'drop',
        ?string $store = null,
        ?string $scope = null,
        int $maxKeys = 50000,
        bool $chunkLocal = false,
    ) {
        $this->key = $key;
        $this->action = $action;
        $this->store = $store;
        $this->scope = $scope;
        $this->maxKeys = $maxKeys;
        $this->chunkLocal = $chunkLocal;
    }

    public function __serialize(): array
    {
        return [
            'seen' => $this->seen,
            'key' => $this->key instanceof Closure ? new SerializableClosure($this->key) : $this->key,
            'action' => $this->action,
            'store' => $this->store,
            'scope' => $this->scope,
            'maxKeys' => $this->maxKeys,
            'chunkLocal' => $this->chunkLocal,
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->seen = $data['seen'] ?? [];
        $this->key = isset($data['key']) && $data['key'] instanceof SerializableClosure
            ? $data['key']->getClosure()
            : ($data['key'] ?? '');
        $this->action = $data['action'] ?? 'drop';
        $this->store = $data['store'] ?? null;
        $this->scope = $data['scope'] ?? null;
        $this->maxKeys = $data['maxKeys'] ?? 50000;
        $this->chunkLocal = $data['chunkLocal'] ?? false;
        $this->pendingClaims = [];
    }

    public function isChunkLocal(): bool
    {
        return $this->chunkLocal;
    }

    public function getStore(): ?string
    {
        return $this->store;
    }

    public function getScope(): ?string
    {
        return $this->scope;
    }

    public function withStore(string $store, ?string $scope = null): self
    {
        $clone = clone $this;
        $clone->store = $store;
        if ($scope !== null) {
            $clone->scope = $scope;
        }

        return $clone;
    }

    public function reset(): void
    {
        $this->seen = [];
        $this->pendingClaims = [];
    }

    public function __clone(): void
    {
        $this->seen = [];
        $this->pendingClaims = [];
    }

    public function computeRecordOwner(RecordContext $record): string
    {
        $identifier = $record->identifier() ?? $record->get('id') ?? $record->get('uuid');
        if ($identifier !== null && is_scalar($identifier) && (string) $identifier !== '') {
            return 'id:'.((string) $identifier).':idx:'.((string) $record->index());
        }

        return 'idx:'.((string) $record->index());
    }

    public function handleRecord(RecordContext $record): RecordContext
    {
        $signature = $this->computeSignature($record);

        if ($signature === null || $signature === '') {
            return $record;
        }

        $hash = hash('xxh128', $signature);
        $recordOwner = $this->computeRecordOwner($record);

        $isDuplicate = false;

        if ($this->store !== null) {
            $cacheKey = ($this->scope ?? 'pipeline:dedup:').$hash;
            $existingClaim = Cache::store($this->store)->get($cacheKey) ?? ($this->seen[$hash] ?? null);

            if ($existingClaim !== null) {
                $existingOwner = is_array($existingClaim)
                    ? ($existingClaim['owner'] ?? null)
                    : (is_string($existingClaim) ? $existingClaim : null);

                if ($existingOwner !== null && (string) $existingOwner === (string) $recordOwner) {
                    $isDuplicate = false;
                    if (! $record->isPreview() && ! $record->isDryRun()) {
                        $this->pendingClaims[$hash] = [
                            'cache_key' => $cacheKey,
                            'hash' => $hash,
                            'owner' => $recordOwner,
                            'index' => $record->index(),
                            'identifier' => $record->identifier(),
                        ];
                    }
                } else {
                    $isDuplicate = true;
                }
            } else {
                $claimData = [
                    'owner' => $recordOwner,
                    'status' => 'pending',
                    'claimed_at' => microtime(true),
                ];

                if (! $record->isPreview() && ! $record->isDryRun()) {
                    Cache::store($this->store)->put($cacheKey, $claimData, 86400);

                    $this->pendingClaims[$hash] = [
                        'cache_key' => $cacheKey,
                        'hash' => $hash,
                        'owner' => $recordOwner,
                        'index' => $record->index(),
                        'identifier' => $record->identifier(),
                    ];
                } else {
                    if (count($this->seen) >= $this->maxKeys) {
                        $this->seen = array_slice($this->seen, (int) ($this->maxKeys * 0.2), null, true);
                    }
                    $this->seen[$hash] = $claimData;
                }
            }
        } else {
            if (isset($this->seen[$hash])) {
                $existing = $this->seen[$hash];
                $existingOwner = is_array($existing)
                    ? ($existing['owner'] ?? null)
                    : (is_string($existing) ? $existing : null);

                if ($existingOwner !== null && (string) $existingOwner === (string) $recordOwner) {
                    $isDuplicate = false;
                    if (! $record->isPreview() && ! $record->isDryRun()) {
                        $this->pendingClaims[$hash] = [
                            'cache_key' => null,
                            'hash' => $hash,
                            'owner' => $recordOwner,
                            'index' => $record->index(),
                            'identifier' => $record->identifier(),
                        ];
                    }
                } else {
                    $isDuplicate = true;
                }
            } else {
                if (count($this->seen) >= $this->maxKeys) {
                    $this->seen = array_slice($this->seen, (int) ($this->maxKeys * 0.2), null, true);
                }
                $this->seen[$hash] = [
                    'owner' => $recordOwner,
                    'status' => 'pending',
                    'claimed_at' => microtime(true),
                ];

                if (! $record->isPreview() && ! $record->isDryRun()) {
                    $this->pendingClaims[$hash] = [
                        'cache_key' => null,
                        'hash' => $hash,
                        'owner' => $recordOwner,
                        'index' => $record->index(),
                        'identifier' => $record->identifier(),
                    ];
                }
            }
        }

        if ($isDuplicate) {
            if ($this->action === 'error') {
                $record->addError('_duplicate', "Duplicate record encountered for key [{$signature}].");
            } else {
                $record->drop();
            }

            return $record;
        }

        return $record;
    }

    /**
     * Commit all pending claims for records that successfully persisted.
     */
    public function commitClaims(): void
    {
        if ($this->store !== null) {
            foreach ($this->pendingClaims as $claim) {
                if ($claim['cache_key'] !== null) {
                    Cache::store($this->store)->put($claim['cache_key'], [
                        'owner' => $claim['owner'],
                        'status' => 'committed',
                        'committed_at' => microtime(true),
                    ], 86400);
                }
            }
        } else {
            foreach ($this->pendingClaims as $hash => $claim) {
                if (isset($this->seen[$hash]) && is_array($this->seen[$hash])) {
                    $this->seen[$hash]['status'] = 'committed';
                    $this->seen[$hash]['committed_at'] = microtime(true);
                }
            }
        }

        $this->pendingClaims = [];
    }

    /**
     * Rollback pending uncommitted claims.
     * If $failedIndicesOrIdentifiers is provided, rolls back only those records.
     * Otherwise rolls back all pending uncommitted claims for this chunk.
     *
     * @param  array<int, int|string>|null  $failedIndicesOrIdentifiers
     */
    public function rollbackClaims(?array $failedIndicesOrIdentifiers = null): void
    {
        if ($failedIndicesOrIdentifiers === null) {
            foreach ($this->pendingClaims as $hash => $claim) {
                if ($this->store !== null && $claim['cache_key'] !== null) {
                    Cache::store($this->store)->forget($claim['cache_key']);
                }
                unset($this->seen[$hash]);
            }
            $this->pendingClaims = [];

            return;
        }

        $filterMap = array_flip(array_map('strval', $failedIndicesOrIdentifiers));

        foreach ($this->pendingClaims as $hash => $claim) {
            $idxMatch = isset($filterMap[(string) $claim['index']]);
            $idMatch = $claim['identifier'] !== null && isset($filterMap[(string) $claim['identifier']]);

            if ($idxMatch || $idMatch) {
                if ($this->store !== null && $claim['cache_key'] !== null) {
                    Cache::store($this->store)->forget($claim['cache_key']);
                }
                unset($this->seen[$hash]);
                unset($this->pendingClaims[$hash]);
            }
        }
    }

    /**
     * @return array<string, array{cache_key: ?string, hash: string, owner: string, index: int|string, identifier: ?string}>
     */
    public function getPendingClaims(): array
    {
        return $this->pendingClaims;
    }

    public function hasClaim(RecordContext|string $recordOrSignature): bool
    {
        $hash = $this->resolveHash($recordOrSignature);
        if ($hash === null) {
            return false;
        }

        if ($this->store !== null) {
            $cacheKey = ($this->scope ?? 'pipeline:dedup:').$hash;

            return Cache::store($this->store)->has($cacheKey) || isset($this->seen[$hash]);
        }

        return isset($this->seen[$hash]);
    }

    public function getClaimOwner(RecordContext|string $recordOrSignature): ?string
    {
        $hash = $this->resolveHash($recordOrSignature);
        if ($hash === null) {
            return null;
        }

        if ($this->store !== null) {
            $cacheKey = ($this->scope ?? 'pipeline:dedup:').$hash;
            $claim = Cache::store($this->store)->get($cacheKey) ?? ($this->seen[$hash] ?? null);
            if ($claim === null) {
                return null;
            }

            return is_array($claim) ? ($claim['owner'] ?? null) : (is_string($claim) ? $claim : null);
        }

        if (! isset($this->seen[$hash])) {
            return null;
        }

        $claim = $this->seen[$hash];

        return is_array($claim) ? ($claim['owner'] ?? null) : (is_string($claim) ? $claim : null);
    }

    public function releaseClaim(RecordContext|string $recordOrSignature): void
    {
        $hash = $this->resolveHash($recordOrSignature);
        if ($hash === null) {
            return;
        }

        if ($this->store !== null) {
            $cacheKey = ($this->scope ?? 'pipeline:dedup:').$hash;
            Cache::store($this->store)->forget($cacheKey);
        }

        unset($this->seen[$hash]);
        unset($this->pendingClaims[$hash]);
    }

    private function resolveHash(RecordContext|string $recordOrSignature): ?string
    {
        if ($recordOrSignature instanceof RecordContext) {
            $sig = $this->computeSignature($recordOrSignature);
            if ($sig === null || $sig === '') {
                return null;
            }

            return hash('xxh128', $sig);
        }

        return hash('xxh128', $recordOrSignature);
    }

    private function computeSignature(RecordContext $record): ?string
    {
        $data = $record->data();

        if ($this->key instanceof Closure) {
            return (string) ($this->key)($data, $record);
        }

        if (is_array($this->key)) {
            $parts = [];
            foreach ($this->key as $col) {
                $val = $data[$col] ?? null;
                if ($val === null) {
                    return null;
                }
                $parts[] = (string) $val;
            }

            return json_encode($parts, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        }

        $val = $data[$this->key] ?? null;

        return $val !== null ? (string) $val : null;
    }

    public function seenCount(): int
    {
        return count($this->seen);
    }
}
