<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Events\PipelineChunkCompleted;
use Robel\DataPipeline\Events\PipelineCompleted;
use Robel\DataPipeline\Events\PipelineFailed;
use Robel\DataPipeline\Events\PipelineRecordFailed;
use Robel\DataPipeline\Events\PipelineStarted;
use Robel\DataPipeline\Exceptions\PipelineValidationException;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Event;

uses(RefreshDatabase::class);

it('dispatches PipelineStarted, PipelineChunkCompleted, and PipelineCompleted events', function () {
    Event::fake([
        PipelineStarted::class,
        PipelineChunkCompleted::class,
        PipelineRecordFailed::class,
        PipelineCompleted::class,
        PipelineFailed::class,
    ]);

    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret'],
        ['name' => 'Bob',   'email' => 'bob@test.com',   'password' => 'secret'],
        ['name' => 'Carol', 'email' => 'carol@test.com', 'password' => 'secret'],
    ];

    $result = DataPipeline::from($rows)
        ->name('Customer Import')
        ->chunk(2)
        ->insert(User::class)
        ->run();

    expect($result->isSuccessful())->toBeTrue();

    Event::assertDispatched(PipelineStarted::class, function (PipelineStarted $event) {
        return $event->name === 'Customer Import' && $event->totalRecords === 3;
    });

    Event::assertDispatched(PipelineChunkCompleted::class, 2); // 2 chunks (2 + 1)

    Event::assertDispatched(PipelineCompleted::class, function (PipelineCompleted $event) {
        return $event->result->successful() === 3 && $event->result->failed() === 0;
    });

    Event::assertNotDispatched(PipelineRecordFailed::class);
    Event::assertNotDispatched(PipelineFailed::class);
});

it('dispatches PipelineRecordFailed when validation errors occur', function () {
    Event::fake([
        PipelineStarted::class,
        PipelineChunkCompleted::class,
        PipelineRecordFailed::class,
        PipelineCompleted::class,
        PipelineFailed::class,
    ]);

    $rows = [
        ['name' => 'Valid User', 'email' => 'valid@test.com', 'password' => 'secret'],
        ['name' => '',           'email' => 'bad-email',      'password' => 'secret'],
    ];

    DataPipeline::from($rows)
        ->validate([
            'name' => ['required'],
            'email' => ['required', 'email'],
        ])
        ->allowPartialSuccess()
        ->insert(User::class)
        ->run();

    Event::assertDispatched(PipelineRecordFailed::class, function (PipelineRecordFailed $event) {
        return $event->failure->rawInput()['email'] === 'bad-email';
    });

    Event::assertDispatched(PipelineCompleted::class, function (PipelineCompleted $event) {
        return $event->result->successful() === 1 && $event->result->failed() === 1;
    });
});

it('dispatches PipelineFailed when failIfAny encounters errors', function () {
    Event::fake([
        PipelineStarted::class,
        PipelineRecordFailed::class,
        PipelineCompleted::class,
        PipelineFailed::class,
    ]);

    $rows = [
        ['name' => '', 'email' => 'bad-email'],
    ];

    try {
        DataPipeline::from($rows)
            ->validate(['name' => ['required']])
            ->failIfAny()
            ->run();
    } catch (PipelineValidationException) {
        // Expected
    }

    Event::assertDispatched(PipelineFailed::class, function (PipelineFailed $event) {
        return $event->exception instanceof PipelineValidationException;
    });

    Event::assertNotDispatched(PipelineCompleted::class);
});
