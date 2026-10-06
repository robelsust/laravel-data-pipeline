<?php

declare(strict_types=1);

return [
    /*
    |--------------------------------------------------------------------------
    | Default Chunk Size
    |--------------------------------------------------------------------------
    |
    | Number of records processed in each streaming batch chunk. Chunks are
    | executed atomically inside isolated database transactions.
    |
    */
    'default_chunk_size' => env('DATA_PIPELINE_CHUNK_SIZE', 500),

    /*
    |--------------------------------------------------------------------------
    | Default Database Connection
    |--------------------------------------------------------------------------
    |
    | Database connection used for pipeline tracking tables (runs, chunks, errors).
    | When null, Laravel's default database connection is used.
    |
    */
    'connection' => env('DATA_PIPELINE_CONNECTION', null),

    /*
    |--------------------------------------------------------------------------
    | Quarantine Redacted Attributes
    |--------------------------------------------------------------------------
    |
    | Sensitive attributes that will be automatically masked in the raw payload
    | when failed records are quarantined in the `pipeline_errors` table.
    |
    */
    'redacted_attributes' => [
        'password',
        'secret',
        'token',
        'credit_card',
        'cvv',
        'ssn',
    ],

    /*
    |--------------------------------------------------------------------------
    | Queue Configuration
    |--------------------------------------------------------------------------
    |
    | Default queue and connection settings when dispatching pipeline chunks
    | across background queue workers using `DataPipeline::queue()`.
    |
    */
    'deduplication' => ['pending_ttl' => 120, 'committed_ttl' => 86400],

    'queue' => [
        'connection' => env('DATA_PIPELINE_QUEUE_CONNECTION', null),
        'name' => env('DATA_PIPELINE_QUEUE_NAME', 'default'),
        'max_retries' => env('DATA_PIPELINE_QUEUE_RETRIES', 30),
        'lease_seconds' => 90,
        'release_delay' => 5,
        'timeout' => env('DATA_PIPELINE_QUEUE_TIMEOUT', 60),
    ],
];
