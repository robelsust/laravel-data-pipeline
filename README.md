# Laravel Data Pipeline (`laravel-data-pipeline`)

<p align="center">
  <img src="https://raw.githubusercontent.com/robelsust/laravel-data-pipeline/main/docs/laravel-data-pipeline-architecture.svg" alt="Laravel Data Pipeline Architecture" width="850">
</p>

<p align="center">
  <a href="https://github.com/robelsust/laravel-data-pipeline/actions"><img src="https://github.com/robelsust/laravel-data-pipeline/actions/workflows/run-tests.yml/badge.svg" alt="Build Status"></a>
  <a href="https://packagist.org/packages/robelsust/laravel-data-pipeline"><img src="https://img.shields.io/packagist/v/robelsust/laravel-data-pipeline.svg" alt="Latest Version"></a>
  <a href="https://packagist.org/packages/robelsust/laravel-data-pipeline"><img src="https://img.shields.io/packagist/php-v/robelsust/laravel-data-pipeline.svg" alt="PHP Version"></a>
  <a href="https://packagist.org/packages/robelsust/laravel-data-pipeline"><img src="https://img.shields.io/badge/laravel-12.x%20|%2013.x-red.svg" alt="Laravel Version"></a>
  <a href="LICENSE.md"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="Software License"></a>
</p>

**Laravel Data Pipeline** is a streaming ETL orchestration package designed for high-throughput batch and stream processing in Laravel.

It eliminates fragile, bespoke loops and solves the common failure modes of large-scale data ingestion: out-of-memory crashes, N+1 validation queries, partial corruption, unhandled poison pills, and unrecoverable mid-stream job crashes.

---

## Key Features

- ⚡ **Bulk Persistence**: Multi-row inserts and upserts; throughput depends on your database, stages, and chunk size.
- 🛡️ **Bounded Streaming Execution**: Synchronous processing buffers one chunk, a bounded deduplication window, and up to 5,000 returned failures. Queue dispatch currently stages all job payloads in memory.
- 🎯 **Batched Database Validation**: `BatchValidator` pre-fetches `unique` and `exists` database constraints in a single chunk query instead of firing individual database queries for every row.
- 🔒 **Atomic Transactional Chunks**: Each chunk executes in an isolated database transaction. If a chunk fails, it rolls back cleanly with zero phantom records.
- ☣️ **Poison Pill Quarantine**: Corrupted records are automatically isolated into the `pipeline_errors` table with redacted raw payloads, record indices, and validation errors without stalling the rest of the stream.
- 🔄 **Surgical Zero-Loss Recovery (`pipeline:retry-failed`)**: Re-process only the quarantined failed records after updating business rules, using transactional row attribution and quarantine cleanup.
- 🛑 **Mid-Stream Crash Recovery (`pipeline:resume`)**: If a worker crashes or gets killed midway, resume cleanly from the exact last committed chunk checkpoint.
- 🚀 **Distributed Queue Scaling (`DataPipeline::queue()`)**: Automatically chunk and fan-out workloads across parallel queue workers with database lease locks.
- 🧪 **Zero-Write Simulation Engine (`dryRun()` & `preview()`)**: Inspect and validate transformed records without writing a single byte to the database.

---

## Historical Benchmark Performance (Embedded Implementation)

These historical results describe the earlier application implementation, not a throughput guarantee for this package release. Run the benchmark against your own schema and infrastructure.

| Benchmark Scenario | Dataset Size | Duration | Throughput | Peak Memory | Result |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **High-Speed Bulk Insert** | 1,000,000 rows | 19.45s | **51,409 rows/sec** | 46 MB | 100% Persisted |
| **Collision Merge (`UpsertSink`)** | 1,000,000 rows (250k old + 750k new) | 30.11s | **33,206 rows/sec** | 48 MB | 250k updated in-place, 750k inserted |
| **Poison Pill Quarantine** | 500,000 rows (2,500 poison pills) | 40.12s | 12,463 rows/sec | 44 MB | 497.5k clean, 2.5k quarantined |
| **Surgical Quarantine Retry** | 2,500 quarantined rows | 0.65s | 3,822 rows/sec | 34 MB | 100% recovered (0 loss) |
| **Crash & Resume Checkpoint** | 500,000 rows (50% crash injection) | 6.56s (resume) | 38,110 rows/sec | 42 MB | Chunks 1–50 skipped, 51–100 completed |

---

## Requirements

- Laravel **12.x** with PHP **8.2+**, or Laravel **13.x** with PHP **8.3+**
- Laravel 11 is no longer supported: its security support ended March 12, 2026. See the [framework support policy](https://laravel.com/framework/docs/13.x/releases#support-policy).
- MySQL 8+, PostgreSQL 14+, or SQLite 3.35+
- For Redis: the PhpRedis extension or `composer require predis/predis` in the host application

---

## Installation

Install the package via Composer:

```bash
composer require robelsust/laravel-data-pipeline
```

Run the database migrations to create the pipeline tracking and quarantine tables:

```bash
php artisan migrate
```

*(Optional)* Publish the configuration file:

```bash
php artisan vendor:publish --tag=data-pipeline-config
```

---

## Quick Start (30 Seconds)

```php
use Robel\DataPipeline\Facades\DataPipeline;

$result = DataPipeline::from(storage_path('imports/users.csv'))
    ->chunk(2500)
    ->mapFields([
        'full_name'  => 'name',
        'user_email' => 'email',
    ])
    ->transform(fn (array $record) => array_merge($record, [
        'email' => strtolower($record['email']),
    ]))
    ->validate([
        'name'  => ['required', 'string', 'max:255'],
        'email' => ['required', 'email', 'unique:users,email'],
    ])
    ->allowPartialSuccess()
    ->track()
    ->upsert('users', uniqueBy: ['email'], update: ['name', 'updated_at'])
    ->run();

echo "Processed {$result->successful} records in {$result->durationInSeconds}s!";
```

---

## Comprehensive Architecture & API Guide

### 1. Sources

DataPipeline accepts any data source that implements `SourceInterface` or native PHP/Laravel iterables:

```php
// 1. Streaming CSV (fgetcsv generator stream)
DataPipeline::from(storage_path('data/catalog.csv'));

// 2. Eloquent Cursor (streaming database rows)
DataPipeline::from(User::query()->where('active', true));

// 3. LazyCollection / PHP Generator
DataPipeline::from(LazyCollection::make(function () {
    yield from fetchExternalApiRows();
}));

// 4. Memory-bounded Custom Source
DataPipeline::from(new CustomWebhookSource($feedUrl));
```

### 2. Processing Stages

Stages are decoupled, single-responsibility transformers executed on every record or chunk:

```php
$pipeline
    // Rename/map column keys
    ->mapFields(['cust_name' => 'name', 'mail' => 'email'])

    // Inline row transformation
    ->transform(function (array $data): array {
        $data['name'] = ucwords(trim($data['name']));
        $data['account_type'] = $data['tier'] > 2 ? 'enterprise' : 'standard';
        return $data;
    })

    // Drop rows that don't match criteria
    ->filter(fn (array $data) => ! empty($data['email']))

    // Batch validation (Zero N+1 database queries)
    ->validate([
        'name'  => ['required', 'string'],
        'email' => ['required', 'email', 'unique:users,email'],
    ])

    // In-stream deduplication (drop repeated keys within chunk or across stream)
    ->deduplicateBy('email', action: 'drop', store: 'redis');
```

### 3. Sinks (Persistence)

Persist processed records cleanly into any database or external service:

```php
// High-speed multi-row bulk insert via QueryBuilder
$pipeline->insert('users');

// Bulk Upsert with conflict resolution (ON DUPLICATE KEY UPDATE)
$pipeline->upsert('users', uniqueBy: ['email'], update: ['name', 'updated_at']);

// Eloquent Model Sink (triggers model events, mutators, and custom casts)
$pipeline->to(new ModelSink(User::class));

// Custom Destination Callback
$pipeline->persist(function (RecordChunk $chunk) {
    Http::post('https://api.warehouse.internal/sync', $chunk->validPayloads())->throw();
});
```

### 4. Resilience & Error Handling

```php
$pipeline
    // Isolate bad rows into quarantine while letting clean rows commit
    ->allowPartialSuccess(true)

    // OR: strict fail-all mode (chunk rollback on any error)
    ->failIfAny()

    // Redact sensitive attributes from quarantine payloads
    ->redactAttributes(['password', 'card_number', 'ssn'])

    // Real-time progress monitoring
    ->onProgress(function (int $processed, ?int $total, int $success, int $failed) {
        echo "Processed {$processed}/{$total} records (Success: {$success}, Failed: {$failed})\n";
    });
```

---

## Artisan CLI Suite

The package includes production management commands out of the box. Resume and retry require a named definition registered in a service provider, or an explicit `--class` / `--definition` option. Register definitions in every worker process.

```php
DataPipeline::register('users-import-v1', fn () => DataPipeline::from(storage_path('imports/users.csv'))
    ->name('users-import-v1')->as('users-import-v1')->track()->insert('users'));
```

### 1. List Recent Pipeline Runs
```bash
php artisan pipeline:list --limit=20 --status=COMPLETED
```

### 2. Inspect Run Diagnostics & Quarantined Errors
```bash
php artisan pipeline:show <run-uuid>
```

### 3. Surgically Retry Quarantined Records
```bash
php artisan pipeline:retry-failed <run-uuid>
```

### 4. Resume an Interrupted or Crashed Run
```bash
php artisan pipeline:resume <run-uuid>
```

### 5. High-Volume Ingestion Benchmark
```bash
php artisan pipeline:benchmark --records=1000000 --chunk-size=5000 --track --upsert
```

### 6. Resilience Stress Testing (Poison Pills & Crashes)
```bash
php artisan pipeline:stress-test all --records=500000 --chunk-size=5000
```

---

## Production configuration and guarantees

The facade resolves `PipelineManager`; synchronous runs, queued jobs, and quarantine retries use the same `ChunkExecutor`. Configure `data-pipeline.connection` before migrations if tracking uses a non-default database.

- Keep sink writes and tracking on the same connection for atomic commits. `allowCrossConnectionTransactions()` permits separate commits and cannot provide distributed atomicity. HTTP calls and other external effects need destination-side idempotency.
- Use a shared lock-capable cache such as Redis for multiple workers. Tracked deduplication ownership is also journaled in `pipeline_deduplication_claims` inside the sink/checkpoint transaction, so committed keys survive cache loss. Untracked in-memory deduplication is a bounded sliding window, and untracked cache entries expire.
- Match worker timeout to `data-pipeline.queue.timeout`. Set queue `retry_after` (or SQS visibility timeout) above the worker timeout, and configure lease duration and pending-claim TTL above the longest chunk execution. Released jobs count as attempts; the default is 30 attempts with a 5-second release delay.
- Passwords and other configured raw attributes are masked before quarantine storage. Retry definitions must restore `[REDACTED]` values from an authoritative source; otherwise those rows remain quarantined. Customize the attribute list for your schema. Database errors stored by built-in sinks contain SQLSTATE, excluding SQL and bindings; custom exception messages and application logs remain the host application's responsibility.
- Partial custom sinks must return a `SinkResult` with explicit persisted/failed row indices or add errors to the failed record contexts. Ambiguous counts cause rollback. Bulk sink failures quarantine the entire chunk; `ModelSink` isolates each row with a savepoint and performs per-row writes.
- `queue()` stages the complete input as job payloads before dispatch and therefore has O(N) dispatch memory. Use synchronous streaming or bounded, independently named runs for very large datasets. Resume retains checkpoint metadata in memory. Sources must be replayable with a stable fingerprint and unchanged chunk size. Stop the original workers before resume and use one resume coordinator per run; active chunk leases are rejected.
- Set a retention policy for completed runs and quarantined data. Deleting a run cascades its checkpoints, errors, and deduplication journal.

Benchmark and stress commands default to `pipeline_benchmark_records`. Stress tests truncate that dedicated table. Truncating a custom table requires `--allow-destructive`; benchmarks additionally require `--truncate`. Generated CSV paths can be configured with `--file` for benchmarks.

For an existing installation, apply the new additive migration during your normal release:

```bash
php artisan migrate --force
php artisan queue:restart
```

## Standalone Package Development & Testing

Run the test suite via Pest:

```bash
composer install
composer test
composer analyse
composer validate --strict
composer audit
```

Real PostgreSQL/Redis crash and concurrency tests use isolated services (database `pipeline_test`, user `pipeline`, password `pipeline-test`):

```bash
PIPELINE_INTEGRATION=1 PIPELINE_PG_PORT=5432 PIPELINE_REDIS_PORT=6379 \
  vendor/bin/pest tests/Feature/RedisPostgresConcurrencyTest.php
```

CI runs the supported PHP/Laravel matrix, static analysis, formatting, dependency audits, and real-service regressions.

Format code via Laravel Pint:

```bash
composer format
```

---

## Security Vulnerabilities

If you discover a security vulnerability within Laravel Data Pipeline, please send an e-mail to Robel at `robelsust@gmail.com`.

---

## License

Laravel Data Pipeline is open-sourced software licensed under the **[MIT license](LICENSE.md)**.
