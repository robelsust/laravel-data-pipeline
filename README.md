# Laravel Data Pipeline (`laravel-data-pipeline`)

<p align="center">
  <img src="https://raw.githubusercontent.com/robelsust/laravel-data-pipeline/main/docs/laravel-data-pipeline-architecture.svg" alt="Laravel Data Pipeline Architecture" width="850">
</p>

<p align="center">
  <a href="https://github.com/robelsust/laravel-data-pipeline/actions"><img src="https://github.com/robelsust/laravel-data-pipeline/actions/workflows/run-tests.yml/badge.svg" alt="Build Status"></a>
  <a href="https://packagist.org/packages/robelsust/laravel-data-pipeline"><img src="https://img.shields.io/packagist/v/robelsust/laravel-data-pipeline.svg" alt="Latest Version"></a>
  <a href="https://packagist.org/packages/robelsust/laravel-data-pipeline"><img src="https://img.shields.io/packagist/php-v/robelsust/laravel-data-pipeline.svg" alt="PHP Version"></a>
  <a href="https://packagist.org/packages/robelsust/laravel-data-pipeline"><img src="https://img.shields.io/badge/laravel-11.x%20|%2012.x%20|%2013.x-red.svg" alt="Laravel Version"></a>
  <a href="LICENSE.md"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="Software License"></a>
</p>

**Laravel Data Pipeline** is an enterprise-grade, memory-bounded ($\mathcal{O}(1)$) ETL orchestration engine designed for high-throughput batch and stream processing in Laravel. 

It eliminates fragile, bespoke loops and solves the common failure modes of large-scale data ingestion: out-of-memory crashes, N+1 validation queries, partial corruption, unhandled poison pills, and unrecoverable mid-stream job crashes.

---

## Key Features

- ⚡ **51,000+ Records/Second Throughput**: Sustained bulk persistence via high-speed query batching and optimized multi-row inserts/upserts.
- 🛡️ **Strict $\mathcal{O}(1)$ Memory Invariance**: Memory ceiling stays flat (< 48 MB) whether processing 10,000 or 10,000,000 records.
- 🎯 **Zero N+1 Validation Queries**: `BatchValidator` pre-fetches `unique` and `exists` database constraints in a single chunk query instead of firing individual database queries for every row.
- 🔒 **Atomic Transactional Chunks**: Each chunk executes in an isolated database transaction. If a chunk fails, it rolls back cleanly with zero phantom records.
- ☣️ **Poison Pill Quarantine**: Corrupted records are automatically isolated into the `pipeline_errors` table with full raw payloads, record indices, and validation errors without stalling the rest of the stream.
- 🔄 **Surgical Zero-Loss Recovery (`pipeline:retry-failed`)**: Re-process only the quarantined failed records after updating business rules, with zero duplicate writes.
- 🛑 **Mid-Stream Crash Recovery (`pipeline:resume`)**: If a worker crashes or gets killed midway, resume cleanly from the exact last committed chunk checkpoint.
- 🚀 **Distributed Queue Scaling (`DataPipeline::queue()`)**: Automatically chunk and fan-out workloads across parallel queue workers with database lease locks.
- 🧪 **Zero-Write Simulation Engine (`dryRun()` & `preview()`)**: Inspect and validate transformed records without writing a single byte to the database.

---

## Benchmark Performance (Verified on MySQL 8)

| Benchmark Scenario | Dataset Size | Duration | Throughput | Peak Memory | Result |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **High-Speed Bulk Insert** | 1,000,000 rows | 19.45s | **51,409 rows/sec** | 46 MB | 100% Persisted |
| **Collision Merge (`UpsertSink`)** | 1,000,000 rows (250k old + 750k new) | 30.11s | **33,206 rows/sec** | 48 MB | 250k updated in-place, 750k inserted |
| **Poison Pill Quarantine** | 500,000 rows (2,500 poison pills) | 40.12s | 12,463 rows/sec | 44 MB | 497.5k clean, 2.5k quarantined |
| **Surgical Quarantine Retry** | 2,500 quarantined rows | 0.65s | 3,822 rows/sec | 34 MB | 100% recovered (0 loss) |
| **Crash & Resume Checkpoint** | 500,000 rows (50% crash injection) | 6.56s (resume) | 38,110 rows/sec | 42 MB | Chunks 1–50 skipped, 51–100 completed |

---

## Requirements

- PHP **8.2**, **8.3**, or **8.4**
- Laravel Framework **11.x**, **12.x**, or **13.x**
- MySQL 8+, PostgreSQL 14+, or SQLite 3.35+

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
// 1. Streaming CSV (SplFileObject generator stream)
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
    ->deduplicate('email', action: 'drop', maxKeys: 50000);
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
    Http::post('https://api.warehouse.internal/sync', $chunk->validPayloads());
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

The package includes production management commands out of the box:

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

## Standalone Package Development & Testing

Run the test suite via Pest:

```bash
composer test
```

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
