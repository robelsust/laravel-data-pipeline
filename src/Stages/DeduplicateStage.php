<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Closure;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Laravel\SerializableClosure\SerializableClosure;
use Robel\DataPipeline\Contracts\RecordStageInterface;
use Robel\DataPipeline\Exceptions\PipelineBusyException;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;

final class DeduplicateStage implements RecordStageInterface
{
    /**
     * @var array<string, array{owner: string, status: string, claimed_at?: float, committed_at?: float}>
     */
    private array $seen = [];

    private ?string $runId = null;

    private string $journalScope = '';

    /** @var array<string, string> */
    private array $durableClaims = [];

    private string|array|Closure $key;

    private string $action;

    private ?string $store;

    private ?string $scope;

    private int $maxKeys;

    private bool $chunkLocal;

    /**
     * @var array<string, array{cache_key: ?string, hash: string, owner: string, index: int|string, identifier: ?string, token?: string}>
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
        if ($maxKeys < 1 || ! in_array($action, ['drop', 'error'], true)) {
            throw new \InvalidArgumentException('Deduplication requires a positive key capacity and a drop or error action.');
        }
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
        $this->durableClaims = [];
        $this->runId = null;
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

    public function prepareChunk(RecordChunk $chunk, ?string $runId, int $position): void
    {
        $this->runId = $runId;
        $this->journalScope = hash('sha256', ($this->scope ?? 'default').':'.$position);
        $this->durableClaims = [];
        if ($runId === null || $this->chunkLocal) {
            return;
        }
        $signatures = [];
        foreach ($chunk->records() as $record) {
            $signature = $this->computeSignature($record);
            if ($signature !== null && $signature !== '') {
                $signatures[] = hash('xxh128', $signature);
            }
        }
        $this->durableClaims = DB::connection(config('data-pipeline.connection'))
            ->table('pipeline_deduplication_claims')
            ->where('pipeline_id', $runId)
            ->where('scope', $this->journalScope)
            ->whereIn('signature', array_unique($signatures))
            ->pluck('record_owner', 'signature')->all();
    }

    /** Persist claim ownership in the same transaction as sink writes and checkpoints. */
    public function recordDurableClaims(): void
    {
        if ($this->runId === null || $this->chunkLocal || $this->pendingClaims === []) {
            return;
        }
        $rows = [];
        foreach ($this->pendingClaims as $hash => $claim) {
            if (! isset($this->durableClaims[$hash])) {
                $rows[] = ['pipeline_id' => $this->runId, 'scope' => $this->journalScope, 'signature' => $hash, 'record_owner' => $claim['owner']];
            }
        }
        if ($rows !== []) {
            DB::connection(config('data-pipeline.connection'))->table('pipeline_deduplication_claims')->insert($rows);
        }
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

        if (isset($this->durableClaims[$hash])) {
            $isDuplicate = $this->durableClaims[$hash] !== $recordOwner;
        } elseif ($this->store !== null) {
            $cacheKey = $this->cacheKey($hash);
            if ($record->isPreview() || $record->isDryRun()) {
                $existing = Cache::store($this->store)->get($cacheKey) ?? ($this->seen[$hash] ?? null);
                $isDuplicate = $existing !== null && ($existing['owner'] ?? null) !== $recordOwner;
                if ($existing === null) {
                    $this->remember($hash, ['owner' => $recordOwner, 'status' => 'pending']);
                }
            } else {
                $isDuplicate = $this->mutateClaim($cacheKey, function () use ($cacheKey, $hash, $recordOwner, $record): bool {
                    $cache = Cache::store($this->store);
                    $existing = $cache->get($cacheKey);
                    if ($existing !== null && ($existing['token'] ?? null) === ($this->pendingClaims[$hash]['token'] ?? '')) {
                        return ($existing['owner'] ?? null) !== $recordOwner;
                    }
                    if ($existing !== null && ($existing['status'] ?? null) === 'committed' && $this->runId === null) {
                        return ($existing['owner'] ?? null) !== $recordOwner;
                    }
                    if ($existing !== null && ($existing['status'] ?? null) === 'pending') {
                        throw new PipelineBusyException('Deduplication key is owned by an active worker.');
                    }
                    if ($existing !== null) {
                        $cache->forget($cacheKey);
                    }
                    $token = (string) Str::uuid();
                    $data = ['owner' => $recordOwner, 'status' => 'pending', 'token' => $token];
                    if (! $cache->add($cacheKey, $data, max(1, (int) config('data-pipeline.deduplication.pending_ttl', 120)))) {
                        throw new PipelineBusyException('Deduplication claim changed while being acquired.');
                    }
                    $this->pendingClaims[$hash] = ['cache_key' => $cacheKey, 'hash' => $hash, 'owner' => $recordOwner, 'index' => $record->index(), 'identifier' => $record->identifier(), 'token' => $token];

                    return false;
                });
            }
        } else {
            if (isset($this->seen[$hash])) {
                $existing = $this->seen[$hash];
                $existingOwner = $existing['owner'];

                if ($existingOwner === $recordOwner) {
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
                    $this->seen = array_slice($this->seen, max(1, (int) ($this->maxKeys * 0.2)), null, true);
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
        foreach ($this->pendingClaims as $hash => $claim) {
            if ($this->store !== null && $claim['cache_key'] !== null) {
                $this->mutateClaim($claim['cache_key'], function () use ($claim): void {
                    $cache = Cache::store($this->store);
                    $existing = $cache->get($claim['cache_key']);
                    if (($existing['token'] ?? null) === ($claim['token'] ?? null)) {
                        $cache->put($claim['cache_key'], ['owner' => $claim['owner'], 'status' => 'committed', 'token' => $claim['token']], max(1, (int) config('data-pipeline.deduplication.committed_ttl', 86400)));
                    }
                });
            } elseif (isset($this->seen[$hash])) {
                $this->seen[$hash]['status'] = 'committed';
            }
        }
        $this->pendingClaims = [];
    }

    /** @param array<int, int|string>|null $failedIndicesOrIdentifiers */
    public function rollbackClaims(?array $failedIndicesOrIdentifiers = null): void
    {
        $filter = $failedIndicesOrIdentifiers === null ? null : array_flip(array_map('strval', $failedIndicesOrIdentifiers));
        foreach ($this->pendingClaims as $hash => $claim) {
            if ($filter !== null && ! isset($filter[(string) $claim['index']]) && ! isset($filter[(string) ($claim['identifier'] ?? '')])) {
                continue;
            }
            if ($this->store !== null && $claim['cache_key'] !== null) {
                $this->mutateClaim($claim['cache_key'], function () use ($claim): void {
                    $cache = Cache::store($this->store);
                    if (($cache->get($claim['cache_key'])['token'] ?? null) === ($claim['token'] ?? null)) {
                        $cache->forget($claim['cache_key']);
                    }
                });
            }
            unset($this->seen[$hash], $this->pendingClaims[$hash]);
        }
    }

    /** @param array{owner: string, status: string} $claim */
    private function remember(string $hash, array $claim): void
    {
        if (count($this->seen) >= $this->maxKeys) {
            $this->seen = array_slice($this->seen, max(1, (int) ($this->maxKeys * 0.2)), null, true);
        }
        $this->seen[$hash] = $claim;
    }

    private function mutateClaim(string $key, Closure $callback): mixed
    {
        try {
            $store = Cache::store($this->store)->getStore();
            if (! $store instanceof LockProvider) {
                throw new \LogicException('Distributed deduplication requires a cache store that supports atomic locks.');
            }

            return $store->lock($key.':mutation', 10)->block(5, $callback);
        } catch (LockTimeoutException $exception) {
            throw new PipelineBusyException('Deduplication claim lock is busy.', previous: $exception);
        }
    }

    /**
     * @return array<string, array{cache_key: ?string, hash: string, owner: string, index: int|string, identifier: ?string, token?: string}>
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
            $cacheKey = $this->cacheKey($hash);

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
            $cacheKey = $this->cacheKey($hash);
            $claim = Cache::store($this->store)->get($cacheKey) ?? ($this->seen[$hash] ?? null);
            if ($claim === null) {
                return null;
            }

            return $claim['owner'];
        }

        if (! isset($this->seen[$hash])) {
            return null;
        }

        $claim = $this->seen[$hash];

        return $claim['owner'];
    }

    public function releaseClaim(RecordContext|string $recordOrSignature): void
    {
        $hash = $this->resolveHash($recordOrSignature);
        if ($hash === null) {
            return;
        }

        if ($this->store !== null) {
            $cacheKey = $this->cacheKey($hash);
            $claim = $this->pendingClaims[$hash] ?? null;
            if ($claim === null) {
                return;
            }
            $this->mutateClaim($cacheKey, function () use ($cacheKey, $claim): void {
                $cache = Cache::store($this->store);
                if (($cache->get($cacheKey)['token'] ?? null) === ($claim['token'] ?? null)) {
                    $cache->forget($cacheKey);
                }
            });
        }

        unset($this->seen[$hash], $this->pendingClaims[$hash]);
    }

    private function cacheKey(string $hash): string
    {
        return ($this->scope ?? 'pipeline:dedup:').($this->runId !== null ? $this->runId.':'.$this->journalScope.':' : '').$hash;
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
