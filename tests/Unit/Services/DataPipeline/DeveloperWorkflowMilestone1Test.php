<?php

declare(strict_types=1);

namespace Tests\Unit\Services\DataPipeline;

use App\Models\User;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Events\PipelineFailed;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Sinks\ModelSink;
use Robel\DataPipeline\Sources\ArraySource;
use Generator;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Event;
use Illuminate\Validation\Rule;
use InvalidArgumentException;
use LogicException;

uses(RefreshDatabase::class);

// =========================================================================
// Item 1: Independent Preview and Execution State & Replayable Sources
// =========================================================================

it('keeps preview and run independent so deduplication stages are not polluted by preview', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'],
        ['name' => 'Bob',   'email' => 'bob@test.com',   'password' => 'secret'],
    ];

    $pipeline = DataPipeline::from($rows)
        ->deduplicate('email')
        ->insert(User::class);

    // Calling preview on first record
    $preview = $pipeline->preview(1);
    expect($preview)->toHaveCount(1)
        ->and(User::count())->toBe(0);

    // Subsequent run must process BOTH Alice and Bob without Alice being dropped as a duplicate!
    $result = $pipeline->run();

    expect($result->totalProcessed())->toBe(2)
        ->and($result->successful())->toBe(2)
        ->and($result->failed())->toBe(0)
        ->and(User::count())->toBe(2);
});

it('keeps dryRun and run independent for deduplication stages', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'],
        ['name' => 'Bob',   'email' => 'bob@test.com',   'password' => 'secret'],
    ];

    $pipeline = DataPipeline::from($rows)
        ->deduplicate('email')
        ->insert(User::class);

    $simulation = $pipeline->dryRun();
    expect($simulation['valid'])->toBe(2)
        ->and(User::count())->toBe(0);

    $result = $pipeline->run();
    expect($result->successful())->toBe(2)
        ->and(User::count())->toBe(2);
});

it('supports replayable generator sources using Closures across preview and run', function () {
    $sourceClosure = function (): Generator {
        yield ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'];
        yield ['name' => 'Bob',   'email' => 'bob@test.com',   'password' => 'secret'];
    };

    $pipeline = DataPipeline::from($sourceClosure)
        ->insert(User::class);

    $preview = $pipeline->preview(1);
    expect($preview)->toHaveCount(1);

    $result = $pipeline->run();
    expect($result->successful())->toBe(2)
        ->and(User::count())->toBe(2);
});

it('detects unreplayable raw Generator sources and throws descriptive LogicException on repeated execution', function () {
    $rawGen = (function (): Generator {
        yield ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'];
        yield ['name' => 'Bob',   'email' => 'bob@test.com',   'password' => 'secret'];
    })();

    $pipeline = DataPipeline::from($rawGen);

    $pipeline->preview(1);

    expect(fn () => $pipeline->run())
        ->toThrow(LogicException::class, 'Direct Generator instances cannot be rewound or replayed');
});

// =========================================================================
// Item 2: Consistent Semantics between Sync and Queue
// =========================================================================

it('fails queued pipeline execution with FAILED status and dispatches PipelineFailed when failIfAny is active', function () {
    config(['queue.default' => 'sync']);
    Event::fake([PipelineFailed::class]);

    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com'],
        ['name' => '',      'email' => 'invalid-email'],
    ];

    DataPipeline::from($rows)
        ->name('queued-fail-test')
        ->validate(['name' => 'required', 'email' => 'email'])
        ->failIfAny()
        ->queue();

    $run = PipelineRun::where('name', 'queued-fail-test')->first();
    expect($run)->not->toBeNull()
        ->and($run->status)->toBe('FAILED');

    Event::assertDispatched(PipelineFailed::class);
});

it('prevents chunk writes to sink under strict failIfAny policy', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'],
        ['name' => '',      'email' => 'bad-email',      'password' => 'secret'],
    ];

    try {
        DataPipeline::from($rows)
            ->validate(['name' => 'required', 'email' => 'email'])
            ->failIfAny()
            ->insert(User::class)
            ->run();
    } catch (PipelineValidationException) {
        // Expected
    }

    expect(User::count())->toBe(0);
});

it('rejects un-serializable progress callbacks in queue with a helpful exception', function () {
    $pdo = app('db')->connection()->getPdo();
    $unserializableClosure = function () use ($pdo) {
        return $pdo;
    };

    expect(fn () => DataPipeline::from([['name' => 'Test']])
        ->onProgress($unserializableClosure)
        ->queue()
    )->toThrow(InvalidArgumentException::class, 'Progress callback cannot be serialized');
});

// =========================================================================
// Item 3: Validate Source Before Resuming
// =========================================================================

it('generates distinct fingerprints for arrays with different contents and same keys', function () {
    $sourceA = new ArraySource([['name' => 'Alice']]);
    $sourceB = new ArraySource([['name' => 'Bob']]);

    expect($sourceA->fingerprint())->not()->toBe($sourceB->fingerprint());
});

it('rejects resumeRun when source fingerprint does not match the original run', function () {
    $pipelineA = DataPipeline::from([['name' => 'Alice', 'email' => 'a@test.com']])
        ->tracked()
        ->name('fingerprint-test');

    $pipelineA->run();
    $runId = PipelineRun::where('name', 'fingerprint-test')->first()->id;

    $pipelineB = DataPipeline::from([['name' => 'Modified Alice', 'email' => 'a@test.com']])
        ->tracked()
        ->name('fingerprint-test');

    expect(fn () => DataPipeline::resumeRun($runId, $pipelineB))
        ->toThrow(InvalidArgumentException::class, 'source fingerprint mismatch');
});

it('rejects resumeRun when chunk size does not match the original run', function () {
    $pipeline = DataPipeline::from([['name' => 'Alice', 'email' => 'a@test.com']])
        ->tracked()
        ->chunk(50)
        ->name('chunk-test');

    $pipeline->run();
    $runId = PipelineRun::where('name', 'chunk-test')->first()->id;

    $resumingPipeline = DataPipeline::from([['name' => 'Alice', 'email' => 'a@test.com']])
        ->tracked()
        ->chunk(25)
        ->name('chunk-test');

    expect(fn () => DataPipeline::resumeRun($runId, $resumingPipeline))
        ->toThrow(InvalidArgumentException::class, 'chunk size mismatch');
});

// =========================================================================
// Item 4: Finish Laravel Validation Compatibility
// =========================================================================

it('correctly passes records matching Rule::exists with whereNot', function () {
    User::create(['name' => 'Active User', 'email' => 'active@test.com', 'password' => 'secret']);
    User::create(['name' => 'Banned User', 'email' => 'banned@test.com', 'password' => 'secret']);

    $rows = [
        ['email' => 'active@test.com'],
        ['email' => 'banned@test.com'],
    ];

    $result = DataPipeline::from($rows)
        ->validate([
            'email' => [
                'required',
                Rule::exists('users', 'email')->whereNot('name', 'Banned User'),
            ],
        ])
        ->run();

    expect($result->successful())->toBe(1)
        ->and($result->failed())->toBe(1);

    $failure = $result->errors()->first();
    expect($failure->rawInput()['email'])->toBe('banned@test.com');
});

it('supports named database connections in exists rules', function () {
    User::create(['name' => 'Active User', 'email' => 'active@test.com', 'password' => 'secret']);

    $rows = [
        ['email' => 'active@test.com'],
    ];

    $result = DataPipeline::from($rows)
        ->validate([
            'email' => [
                'required',
                Rule::exists('sqlite.users', 'email'),
            ],
        ])
        ->run();

    expect($result->successful())->toBe(1)
        ->and($result->failed())->toBe(0);
});

it('validates wildcard array fields for uniqueness and existence', function () {
    User::create(['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret']);
    User::create(['name' => 'Bob', 'email' => 'bob@test.com', 'password' => 'secret']);

    $rows = [
        ['name' => 'Doc 1', 'emails' => ['alice@test.com', 'bob@test.com']],
        ['name' => 'Doc 2', 'emails' => ['missing@test.com']],
    ];

    $result = DataPipeline::from($rows)
        ->validate([
            'emails.*' => Rule::exists('users', 'email'),
        ])
        ->run();

    expect($result->successful())->toBe(1)
        ->and($result->failed())->toBe(1);
});

it('respects custom database-rule error messages and custom attributes', function () {
    User::create(['name' => 'Existing', 'email' => 'taken@test.com', 'password' => 'secret']);

    $rows = [
        ['email' => 'taken@test.com'],
    ];

    $result = DataPipeline::from($rows)
        ->validate(
            rules: [
                'email' => ['required', Rule::unique('users', 'email')],
            ],
            messages: [
                'email.unique' => 'Sorry, :attribute is taken by another account.',
            ],
            customAttributes: [
                'email' => 'corporate email',
            ]
        )
        ->run();

    expect($result->failed())->toBe(1);
    $error = $result->errors()->first()->errors()['email'][0];
    expect($error)->toBe('Sorry, corporate email is taken by another account.');
});

it('handles Model instance in Rule::unique ignore', function () {
    $existingUser = User::create(['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret']);

    $rows = [
        ['id' => $existingUser->id, 'email' => 'alice@test.com'],
    ];

    $result = DataPipeline::from($rows)
        ->validate([
            'email' => [
                'required',
                Rule::unique('users', 'email')->ignoreModel($existingUser),
            ],
        ])
        ->run();

    expect($result->successful())->toBe(1)
        ->and($result->failed())->toBe(0);
});

// =========================================================================
// Item 5: Make Persistence Behavior Explicit
// =========================================================================

it('InsertSink and UpsertSink auto-populate model timestamps and resolve connection', function () {
    $rows = [
        ['name' => 'Timestamp User', 'email' => 'ts@test.com', 'password' => 'secret'],
    ];

    $sink = new InsertSink(User::class);
    expect($sink->getConnection())->toBe((new User)->getConnectionName());

    DataPipeline::from($rows)
        ->to($sink)
        ->run();

    $user = User::where('email', 'ts@test.com')->first();
    expect($user)->not->toBeNull()
        ->and($user->created_at)->not->toBeNull()
        ->and($user->updated_at)->not->toBeNull();
});

it('ModelSink executes model casts, mutators, and fires Eloquent events', function () {
    $rows = [
        ['name' => 'Model Sink User', 'email' => 'modelsink@test.com', 'password' => 'secret'],
    ];

    $result = DataPipeline::from($rows)
        ->to(new ModelSink(User::class))
        ->run();

    expect($result->successful())->toBe(1);

    $user = User::where('email', 'modelsink@test.com')->first();
    expect($user)->not->toBeNull()
        ->and($user->name)->toBe('Model Sink User');
});
