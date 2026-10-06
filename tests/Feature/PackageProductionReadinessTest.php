<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Robel\DataPipeline\ChunkExecutor;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Exceptions\PipelineBusyException;
use Robel\DataPipeline\Facades\DataPipeline as PipelineFacade;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\RecordContext;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Sinks\CustomSink;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(RefreshDatabase::class);

it('executes the documented facade entry point through the registered manager', function () {
    $result = PipelineFacade::from([['name' => 'Facade User', 'email' => 'facade@test.com', 'password' => 'test']])->insert(User::class)->run();

    expect($result->successful)->toBe(1);
    $this->assertDatabaseHas('users', ['email' => 'facade@test.com']);
});

it('applies configured chunk size and automatic quarantine redaction', function () {
    config(['data-pipeline.default_chunk_size' => 1, 'data-pipeline.redacted_attributes' => ['password', 'token']]);
    $result = DataPipeline::from([['email' => 'invalid', 'password' => 'private', 'token' => 'private'], ['email' => 'invalid']])->track()->validate(['email' => 'email'])->run();

    expect($result->failed)->toBe(2);
    expect(PipelineChunk::pluck('records_count')->all())->toBe([1, 1]);
    expect(PipelineError::first()->raw_payload)->toMatchArray(['password' => '[REDACTED]', 'token' => '[REDACTED]']);
});

it('uses the configured tracking connection for migrations models and repository writes', function () {
    config(['database.connections.tracking' => ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => ''], 'data-pipeline.connection' => 'tracking']);
    Artisan::call('migrate', ['--database' => 'tracking', '--path' => realpath(__DIR__.'/../../database/migrations'), '--realpath' => true, '--force' => true]);

    DataPipeline::from([['email' => 'invalid']])->track()->validate(['email' => 'email'])->run();

    expect(PipelineRun::count())->toBe(1);
    expect(DB::connection('tracking')->table('pipeline_errors')->count())->toBe(1);
    expect(DB::connection('sqlite')->table('pipeline_runs')->count())->toBe(0);
    DB::purge('tracking');
});

it('rejects atomic tracked writes across different connections by default', function () {
    config(['data-pipeline.connection' => 'tracking']);

    expect(fn () => app(ChunkExecutor::class)->execute(new RecordChunk([]), [], new InsertSink(User::class), app(PipelineRepositoryInterface::class), 'run'))
        ->toThrow(LogicException::class, 'dual-connection atomicity');
});

it('rolls back sink rows and deduplication claims when a checkpoint cannot be saved', function () {
    $repository = app(PipelineRepositoryInterface::class);
    $run = $repository->createRun('ArraySource', 1);
    $fail = true;
    PipelineChunk::updating(function (PipelineChunk $checkpoint) use (&$fail): void {
        if ($fail && $checkpoint->status === 'COMPLETED') {
            $fail = false;
            throw new RuntimeException('Checkpoint write failed');
        }
    });
    $job = new ProcessPipelineChunkJob($run->id, 1, [['index' => 0, 'raw' => ['name' => 'Retry', 'email' => 'retry@test.com', 'password' => 'test']]], stages: [new DeduplicateStage('email', store: 'array')], sink: new InsertSink(User::class));

    expect(fn () => $job->handle($repository))->toThrow(RuntimeException::class, 'Checkpoint write failed');
    $this->assertDatabaseCount('users', 0);
    $this->assertDatabaseCount('pipeline_deduplication_claims', 0);
    $job->handle($repository);

    $this->assertDatabaseCount('users', 1);
    expect(PipelineChunk::first()->status)->toBe('COMPLETED');
});

it('retains committed deduplication after Redis data is lost', function () {
    $repository = app(PipelineRepositoryInterface::class);
    $run = $repository->createRun('ArraySource', 2);
    $makeJob = fn (int $sequence): ProcessPipelineChunkJob => new ProcessPipelineChunkJob($run->id, $sequence, [['index' => $sequence, 'raw' => ['name' => 'Same key', 'email' => 'user'.$sequence.'@test.com', 'password' => 'test']]], stages: [new DeduplicateStage('name', store: 'array')], sink: new InsertSink(User::class));
    $makeJob(1)->handle($repository);
    Cache::store('array')->flush();

    $makeJob(2)->handle($repository);

    $this->assertDatabaseCount('users', 1);
    expect(PipelineChunk::where('sequence', 2)->value('successful_count'))->toBe(0);
});

it('releases claims for records rejected by a partially successful sink', function () {
    $repository = app(PipelineRepositoryInterface::class);
    $run = $repository->createRun('ArraySource', 3);
    $sink = new CustomSink(function (RecordChunk $chunk): SinkResult {
        $rows = $chunk->records();
        DB::table('users')->insert($rows[0]->data());
        $rows[1]->addError('_sink', 'Transient row failure');

        return SinkResult::partial(1, 1, persistedIndices: [0], failedIndices: [1]);
    });
    $rows = [['index' => 0, 'raw' => ['name' => 'First', 'email' => 'first@test.com', 'password' => 'test']], ['index' => 1, 'raw' => ['name' => 'Second', 'email' => 'second@test.com', 'password' => 'test']]];
    (new ProcessPipelineChunkJob($run->id, 1, $rows, stages: [new DeduplicateStage('name', store: 'array')], sink: $sink))->handle($repository);

    (new ProcessPipelineChunkJob($run->id, 2, [['index' => 2, 'raw' => $rows[1]['raw']]], stages: [new DeduplicateStage('name', store: 'array')], sink: new InsertSink(User::class)))->handle($repository);

    $this->assertDatabaseCount('users', 2);
    $this->assertDatabaseCount('pipeline_deduplication_claims', 2);
});

it('keeps stress-test data separate from application users by default', function () {
    $user = User::factory()->create();

    $this->artisan('pipeline:stress-test', ['scenario' => 'crash-resume', '--records' => 100, '--chunk-size' => 20])->assertSuccessful();

    $this->assertModelExists($user);
    $this->assertDatabaseCount('users', 1);
    $this->assertDatabaseCount('pipeline_benchmark_records', 100);
});

it('refuses destructive stress tests on a custom table without explicit opt-in', function () {
    $user = User::factory()->create();

    $this->artisan('pipeline:stress-test', ['--table' => 'users', '--records' => 100])->expectsOutputToContain('--allow-destructive')->assertFailed();

    $this->assertModelExists($user);
});

it('applies configured queue connection name timeout and retry settings', function () {
    config(['data-pipeline.queue.connection' => 'database', 'data-pipeline.queue.name' => 'imports', 'data-pipeline.queue.timeout' => 45, 'data-pipeline.queue.max_retries' => 7]);
    Bus::fake();

    DataPipeline::from([['email' => 'queue@test.com']])->queue();

    Bus::assertBatched(function ($batch): bool {
        return $batch->options['connection'] === 'database' && $batch->options['queue'] === 'imports' && $batch->jobs[0]->timeout === 45 && $batch->jobs[0]->tries === 7;
    });
});

it('does not reserve deduplication keys for records dropped by a later filter', function () {
    $result = DataPipeline::from([
        ['name' => 'Dropped', 'email' => 'same@test.com', 'password' => 'test', 'keep' => false],
        ['name' => 'Kept', 'email' => 'same@test.com', 'password' => 'test', 'keep' => true],
    ])->chunk(1)->track()->deduplicateBy('email', store: 'array')
        ->filter(fn (array $row): bool => $row['keep'])
        ->transform(function (array $row): array {
            unset($row['keep']);

            return $row;
        })
        ->insert(User::class)->run();

    expect($result->successful)->toBe(1);
    $this->assertDatabaseHas('users', ['name' => 'Kept']);
    $this->assertDatabaseCount('pipeline_deduplication_claims', 1);
});

it('cannot release a distributed claim belonging to another stage instance', function () {
    $owner = new DeduplicateStage('email', store: 'array');
    $other = new DeduplicateStage('email', store: 'array');
    $record = RecordContext::fromRaw(['email' => 'owned@test.com'], 0);
    $owner->handleRecord($record);

    $other->releaseClaim($record);

    expect($owner->hasClaim($record))->toBeTrue();
    expect(fn () => $other->handleRecord(RecordContext::fromRaw(['email' => 'owned@test.com'], 1)))
        ->toThrow(PipelineBusyException::class);
    $owner->rollbackClaims();
});

it('does not let a stale terminal failure overwrite a committed checkpoint', function () {
    $repository = app(PipelineRepositoryInterface::class);
    $run = $repository->createRun('ArraySource', 1);
    $job = new ProcessPipelineChunkJob($run->id, 1, [['index' => 0, 'raw' => ['name' => 'Committed', 'email' => 'committed@test.com', 'password' => 'test']]], sink: new InsertSink(User::class));
    $job->handle($repository);

    $job->failed(new RuntimeException('Stale delivery failed'));

    $this->assertDatabaseHas('pipeline_chunks', ['status' => 'COMPLETED']);
    expect($run->fresh()->status)->toBe('RUNNING');
    $this->assertDatabaseCount('users', 1);
});

it('keeps SQL bindings out of quarantined database failure diagnostics', function () {
    config(['data-pipeline.redacted_attributes' => ['password']]);
    User::factory()->create(['email' => 'duplicate@test.com']);

    $result = DataPipeline::from([['name' => 'Duplicate', 'email' => 'duplicate@test.com', 'password' => 'private-password-value']])->track()->insert(User::class)->run();

    expect($result->failed)->toBe(1);
    expect(json_encode(PipelineError::first()->toArray()))->not->toContain('private-password-value');
    expect(PipelineError::first()->validation_errors['_db'][0])->toContain('SQLSTATE');
});

it('keeps redacted quarantine rows until a retry definition restores their values', function () {
    config(['data-pipeline.redacted_attributes' => ['password']]);
    DataPipeline::from([['name' => 'Restored', 'email' => 'invalid', 'password' => 'private']])->track()->validate(['email' => 'email'])->insert(User::class)->run();
    $run = PipelineRun::first();
    $definition = DataPipeline::from([])->transform(function (array $row): array {
        $row['email'] = 'restored@test.com';

        return $row;
    })->insert(User::class);

    $first = DataPipeline::retryFailedRecords($run->id, $definition);

    expect($first['recovered'])->toBe(0);
    $this->assertDatabaseCount('users', 0);
    $this->assertDatabaseCount('pipeline_errors', 1);
    $definition->transform(function (array $row): array {
        $row['password'] = 'restored-value';

        return $row;
    });
    $second = DataPipeline::retryFailedRecords($run->id, $definition);

    expect($second['recovered'])->toBe(1);
    $this->assertDatabaseHas('users', ['password' => 'restored-value']);
    $this->assertDatabaseCount('pipeline_errors', 0);
});

it('does not replay committed zero-write chunks during resume', function () {
    $rows = [['name' => 'Filtered', 'email' => 'filtered@test.com', 'password' => 'test']];
    $calls = 0;
    $pipeline = DataPipeline::from($rows)->track()->filter(function (array $row) use (&$calls): bool {
        $calls++;

        return false;
    })->insert(User::class);
    $pipeline->run();
    $run = PipelineRun::first();

    $result = DataPipeline::resumeRun($run->id, $pipeline);

    expect($calls)->toBe(1);
    expect($result->totalProcessed)->toBe(1);
    $this->assertDatabaseCount('users', 0);
});

it('refuses to resume while an active worker owns a chunk', function () {
    $repository = app(PipelineRepositoryInterface::class);
    $pipeline = DataPipeline::from([])->track();
    $run = $repository->createRun('ArraySource', 0);
    PipelineChunk::create(['pipeline_id' => $run->id, 'sequence' => 1, 'status' => 'PROCESSING', 'started_at' => now()]);

    expect(fn () => DataPipeline::resumeRun($run->id, $pipeline))
        ->toThrow(PipelineBusyException::class, 'active chunk lease');
});
