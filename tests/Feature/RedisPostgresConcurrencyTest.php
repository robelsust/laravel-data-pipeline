<?php

declare(strict_types=1);

use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Foundation\Testing\DatabaseMigrations;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Sinks\CustomSink;
use Robel\DataPipeline\Sinks\InsertSink;
use Robel\DataPipeline\Sinks\ModelSink;
use Robel\DataPipeline\Stages\DeduplicateStage;
use Robel\DataPipeline\Tests\Fixtures\User;

uses(DatabaseMigrations::class);

beforeEach(function () {
    if (getenv('PIPELINE_INTEGRATION') !== '1' || ! function_exists('pcntl_fork')) {
        $this->markTestSkipped('Set PIPELINE_INTEGRATION=1 with dedicated PostgreSQL and Redis services.');
    }
});

function forkPipelineWorker(Closure $work): int
{
    DB::disconnect();
    Redis::connection()->disconnect();
    $pid = pcntl_fork();
    if ($pid === -1) {
        throw new RuntimeException('Unable to fork integration worker.');
    }
    if ($pid === 0) {
        try {
            $work();
            exit(0);
        } catch (Throwable $exception) {
            fwrite(STDERR, $exception->getMessage().PHP_EOL);
            exit(1);
        }
    }

    return $pid;
}

function waitForPipelineWorker(int $pid, bool $killed = false): void
{
    pcntl_waitpid($pid, $status);
    if ($killed) {
        expect(pcntl_wifsignaled($status))->toBeTrue();
        expect(pcntl_wtermsig($status))->toBe(SIGKILL);
    } else {
        expect(pcntl_wifexited($status))->toBeTrue();
        expect(pcntl_wexitstatus($status))->toBe(0);
    }
}

/** @return array<int, array{index: int, raw: array<string, string>}> */
function concurrentPipelineRows(int $index): array
{
    return [['index' => $index, 'raw' => ['name' => 'Shared key', 'email' => "worker{$index}@test.com", 'password' => 'test']]];
}

it('releases a competing worker and deduplicates its redelivery after the owner commits', function () {
    Cache::store('redis')->flush();
    $run = app(PipelineRepositoryInterface::class)->createRun('concurrent-source', 2);
    [$parent, $child] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    stream_set_timeout($parent, 10);
    stream_set_timeout($child, 10);
    $pid = forkPipelineWorker(function () use ($run, $parent, $child): void {
        fclose($parent);
        $sink = new CustomSink(function (RecordChunk $chunk) use ($child): SinkResult {
            DB::table('users')->insert($chunk->valid()[0]->data());
            fwrite($child, "claimed\n");
            if (fgets($child) !== "commit\n") {
                throw new RuntimeException('Parent did not release the commit barrier.');
            }

            return SinkResult::success(1);
        });
        (new ProcessPipelineChunkJob($run->id, 1, concurrentPipelineRows(1), stages: [new DeduplicateStage('name', store: 'redis')], sink: $sink))->handle(app(PipelineRepositoryInterface::class));
    });
    fclose($child);
    $claimed = false;
    try {
        $message = fgets($parent);
        $claimed = $message === "claimed\n";
        expect($message)->toBe("claimed\n");
        $competing = new ProcessPipelineChunkJob($run->id, 2, concurrentPipelineRows(2), stages: [new DeduplicateStage('name', store: 'redis')], sink: new InsertSink('users'));

        $competing->withFakeQueueInteractions()->handle(app(PipelineRepositoryInterface::class));

        $competing->assertReleased(5);
        $this->assertDatabaseCount('users', 0);
    } finally {
        if ($claimed) {
            fwrite($parent, "commit\n");
        }
        fclose($parent);
        waitForPipelineWorker($pid);
    }
    $redelivery = new ProcessPipelineChunkJob($run->id, 2, concurrentPipelineRows(2), stages: [new DeduplicateStage('name', store: 'redis')], sink: new InsertSink('users'));
    $redelivery->handle(app(PipelineRepositoryInterface::class));

    $this->assertDatabaseCount('users', 1);
    $this->assertDatabaseCount('pipeline_deduplication_claims', 1);
    $this->assertDatabaseHas('pipeline_chunks', ['sequence' => 2, 'status' => 'COMPLETED', 'successful_count' => 0]);
})->group('integration');

it('recovers a killed worker after its database transaction rolls back and its lease expires', function () {
    Cache::store('redis')->flush();
    $run = app(PipelineRepositoryInterface::class)->createRun('crash-before-commit', 1);
    $pid = forkPipelineWorker(function () use ($run): void {
        $sink = new CustomSink(function (RecordChunk $chunk): never {
            DB::table('users')->insert($chunk->valid()[0]->data());
            posix_kill(getmypid(), SIGKILL);
            exit(1);
        });
        (new ProcessPipelineChunkJob($run->id, 1, concurrentPipelineRows(1), stages: [new DeduplicateStage('name', store: 'redis')], sink: $sink))->handle(app(PipelineRepositoryInterface::class));
    });
    waitForPipelineWorker($pid, killed: true);
    $this->assertDatabaseCount('users', 0);
    $this->assertDatabaseCount('pipeline_deduplication_claims', 0);
    $job = new ProcessPipelineChunkJob($run->id, 1, concurrentPipelineRows(1), stages: [new DeduplicateStage('name', store: 'redis')], sink: new InsertSink('users'));

    $job->withFakeQueueInteractions()->handle(app(PipelineRepositoryInterface::class));
    $job->assertReleased(5);
    PipelineChunk::query()->update(['started_at' => now()->subMinutes(5)]);
    Cache::store('redis')->flush();
    $job->handle(app(PipelineRepositoryInterface::class));

    $this->assertDatabaseCount('users', 1);
    $this->assertDatabaseHas('pipeline_chunks', ['status' => 'COMPLETED', 'successful_count' => 1]);
})->group('integration');

it('retains durable deduplication when a worker dies after database commit before cache commit', function () {
    Cache::store('redis')->flush();
    $run = app(PipelineRepositoryInterface::class)->createRun('crash-after-commit', 2);
    $pid = forkPipelineWorker(function () use ($run): void {
        Event::listen(TransactionCommitted::class, function (TransactionCommitted $event): void {
            if ($event->connection->transactionLevel() === 0) {
                posix_kill(getmypid(), SIGKILL);
            }
        });
        (new ProcessPipelineChunkJob($run->id, 1, concurrentPipelineRows(1), stages: [new DeduplicateStage('name', store: 'redis')], sink: new InsertSink('users')))->handle(app(PipelineRepositoryInterface::class));
    });
    waitForPipelineWorker($pid, killed: true);
    $this->assertDatabaseCount('users', 1);
    $this->assertDatabaseCount('pipeline_deduplication_claims', 1);

    (new ProcessPipelineChunkJob($run->id, 1, concurrentPipelineRows(1), stages: [new DeduplicateStage('name', store: 'redis')], sink: new InsertSink('users')))->handle(app(PipelineRepositoryInterface::class));
    (new ProcessPipelineChunkJob($run->id, 2, concurrentPipelineRows(2), stages: [new DeduplicateStage('name', store: 'redis')], sink: new InsertSink('users')))->handle(app(PipelineRepositoryInterface::class));
    Cache::store('redis')->flush();
    (new ProcessPipelineChunkJob($run->id, 3, concurrentPipelineRows(3), stages: [new DeduplicateStage('name', store: 'redis')], sink: new InsertSink('users')))->handle(app(PipelineRepositoryInterface::class));

    $this->assertDatabaseCount('users', 1);
    $this->assertDatabaseHas('pipeline_chunks', ['sequence' => 3, 'status' => 'COMPLETED', 'successful_count' => 0]);
})->group('integration');

it('commits clean model rows after a PostgreSQL constraint rejects another row', function () {
    DB::table('users')->insert(['name' => 'Existing', 'email' => 'existing@test.com', 'password' => 'test']);

    $result = DataPipeline::from([
        ['name' => 'Duplicate', 'email' => 'existing@test.com', 'password' => 'test'],
        ['name' => 'Clean', 'email' => 'clean@test.com', 'password' => 'test'],
    ])->track()->to(new ModelSink(User::class))->run();

    expect($result->successful)->toBe(1);
    expect($result->failed)->toBe(1);
    $this->assertDatabaseCount('users', 2);
    $this->assertDatabaseHas('pipeline_chunks', ['status' => 'PARTIAL', 'successful_count' => 1, 'failed_count' => 1]);
    $this->assertDatabaseCount('pipeline_errors', 1);
})->group('integration');

it('quarantines a failed PostgreSQL bulk insert without aborting checkpoint persistence', function () {
    DB::table('users')->insert(['name' => 'Existing', 'email' => 'existing@test.com', 'password' => 'test']);

    $result = DataPipeline::from([
        ['name' => 'Duplicate', 'email' => 'existing@test.com', 'password' => 'test'],
        ['name' => 'Clean', 'email' => 'clean@test.com', 'password' => 'test'],
    ])->track()->insert('users')->run();

    expect($result->successful)->toBe(0);
    expect($result->failed)->toBe(2);
    $this->assertDatabaseCount('users', 1);
    $this->assertDatabaseHas('pipeline_chunks', ['status' => 'FAILED', 'failed_count' => 2]);
    $this->assertDatabaseCount('pipeline_errors', 2);
})->group('integration');

it('skips excluded database fields before issuing PostgreSQL existence queries', function () {
    $result = DataPipeline::from([[
        'name' => 'Admin', 'email' => 'admin@test.com', 'password' => 'test',
        'role' => 'admin', 'manager_id' => 'not-an-integer',
    ]])->validate(['manager_id' => 'exclude_if:role,admin|exists:users,id'])
        ->transform(fn (array $row): array => array_intersect_key($row, array_flip(['name', 'email', 'password'])))
        ->track()->insert('users')->run();

    expect($result->successful)->toBe(1);
    $this->assertDatabaseHas('users', ['email' => 'admin@test.com']);
})->group('integration');

it('processes serialized Redis batch jobs through a worker and completes the run once', function () {
    Cache::store('redis')->flush();
    config([
        'queue.batching.database' => 'pgsql',
        'queue.connections.redis' => [
            'driver' => 'redis', 'connection' => 'default', 'queue' => 'pipeline_test',
            'retry_after' => 120, 'block_for' => null, 'after_commit' => false,
        ],
        'data-pipeline.queue.connection' => 'redis',
        'data-pipeline.queue.name' => 'pipeline_test',
    ]);
    $runId = DataPipeline::from([
        ['name' => 'One', 'email' => 'one@test.com', 'password' => 'test'],
        ['name' => 'Two', 'email' => 'two@test.com', 'password' => 'test'],
        ['name' => 'Three', 'email' => 'three@test.com', 'password' => 'test'],
    ])->chunk(1)->transform(fn (array $row): array => array_replace($row, ['name' => strtoupper($row['name'])]))
        ->deduplicateBy('email', store: 'redis')->insert('users')->queue();
    $pid = forkPipelineWorker(function (): void {
        Artisan::call('queue:work', [
            'connection' => 'redis', '--queue' => 'pipeline_test', '--stop-when-empty' => true,
            '--sleep' => 0, '--tries' => 30, '--timeout' => 60,
        ]);
    });

    waitForPipelineWorker($pid);

    $this->assertDatabaseCount('users', 3);
    $this->assertDatabaseHas('users', ['name' => 'THREE']);
    $this->assertDatabaseHas('pipeline_runs', ['id' => $runId, 'status' => 'COMPLETED', 'successful_records' => 3]);
    $this->assertDatabaseCount('pipeline_chunks', 3);
    $this->assertDatabaseCount('pipeline_deduplication_claims', 3);
    $this->assertDatabaseCount('failed_jobs', 0);
})->group('integration');
