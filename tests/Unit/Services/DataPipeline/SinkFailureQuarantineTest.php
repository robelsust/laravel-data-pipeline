<?php

declare(strict_types=1);

use Illuminate\Foundation\Testing\RefreshDatabase;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Queue\ProcessPipelineChunkJob;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Robel\DataPipeline\Results\SinkResult;
use Robel\DataPipeline\Sinks\CustomSink;

uses(RefreshDatabase::class);

it('captures sink failures and preserves them in RecordErrorBag and quarantine during synchronous execution', function () {
    $rows = [
        ['id' => 1, 'email' => 'record1@test.com'],
        ['id' => 2, 'email' => 'record2@test.com'],
    ];

    $customSink = new CustomSink(function ($chunk) {
        $valid = $chunk->valid();
        // Simulate sink failing record 2
        foreach ($valid as $record) {
            if ($record->get('email') === 'record2@test.com') {
                $record->addError('gateway', 'Card declined by merchant provider.');
            }
        }

        return SinkResult::partial(1, 1, ['declined' => 'Card declined']);
    });

    $result = DataPipeline::from($rows)
        ->track(true)
        ->allowPartialSuccess(true)
        ->to($customSink)
        ->run();

    expect($result->successful())->toBe(1)
        ->and($result->failed())->toBe(1)
        ->and($result->hasFailures())->toBeTrue()
        ->and($result->errors()->count())->toBe(1);

    $failure = $result->errors()->for(1); // Index 1 is record 2
    expect($failure)->not->toBeNull()
        ->and($failure->errors())->toHaveKey('gateway');

    // Quarantine verification
    $runId = $result->metadata['run_id'] ?? null;
    expect($runId)->not->toBeNull();

    $quarantined = PipelineError::where('pipeline_id', $runId)->get();
    expect($quarantined)->toHaveCount(1)
        ->and($quarantined->first()->raw_payload['email'])->toBe('record2@test.com');
});

it('captures sink failures and quarantines them during queued worker execution', function () {
    $repo = app(PipelineRepositoryInterface::class);
    $run = $repo->createRun(
        sourceType: 'ArraySource',
        totalRecords: 2,
        name: 'Queue Sink Failure Test'
    );

    $customSink = new CustomSink(function ($chunk) {
        $valid = $chunk->valid();
        if (! empty($valid)) {
            $valid[0]->addError('_sink', 'Database lock timeout.');

            return SinkResult::partial(count($valid) - 1, 1, ['exception' => 'Database lock timeout.']);
        }

        return SinkResult::success(0);
    });

    $job = new ProcessPipelineChunkJob(
        runId: $run->id,
        sequence: 1,
        rawRecords: [
            ['index' => 1, 'raw' => ['email' => 'job_fail@test.com']],
        ],
        sink: $customSink,
    );

    $job->handle($repo);

    $quarantined = $repo->getQuarantinedRecords($run->id);
    expect($quarantined)->toHaveCount(1)
        ->and($quarantined[1]['email'])->toBe('job_fail@test.com');

    $runFresh = $repo->findRun($run->id);
    expect($runFresh->failed_records)->toBe(1)
        ->and($runFresh->successful_records)->toBe(0);
});
