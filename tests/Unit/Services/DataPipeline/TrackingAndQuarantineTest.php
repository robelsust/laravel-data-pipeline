<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

it('persists pipeline run, chunk checkpoints, and quarantined errors when tracking is enabled', function () {
    $rows = [
        ['name' => 'Alice', 'email' => 'alice@test.com', 'password' => 'secret123'],
        ['name' => '',      'email' => 'invalid-email',  'password' => 'supersecret456'],
        ['name' => 'Charlie', 'email' => 'charlie@test.com', 'password' => 'secret789'],
    ];

    $result = DataPipeline::from($rows)
        ->name('User Onboarding Test')
        ->chunk(2)
        ->track()
        ->redactAttributes(['password'])
        ->validate([
            'name' => ['required'],
            'email' => ['required', 'email'],
        ])
        ->insert(User::class)
        ->run();

    $runId = $result->metadata['run_id'] ?? null;
    expect($runId)->not->toBeNull();

    // 1. Verify PipelineRun record
    $run = PipelineRun::find($runId);
    expect($run)->not->toBeNull()
        ->and($run->name)->toBe('User Onboarding Test')
        ->and($run->status)->toBe('PARTIAL_SUCCESS')
        ->and($run->total_records)->toBe(3)
        ->and($run->processed_records)->toBe(3)
        ->and($run->successful_records)->toBe(2)
        ->and($run->failed_records)->toBe(1);

    // 2. Verify PipelineChunk checkpoints
    $chunks = PipelineChunk::where('pipeline_id', $runId)->orderBy('sequence')->get();
    expect($chunks)->toHaveCount(2)
        ->and($chunks[0]->sequence)->toBe(1)
        ->and($chunks[0]->records_count)->toBe(2)
        ->and($chunks[0]->successful_count)->toBe(1)
        ->and($chunks[0]->failed_count)->toBe(1)
        ->and($chunks[1]->sequence)->toBe(2)
        ->and($chunks[1]->records_count)->toBe(1)
        ->and($chunks[1]->successful_count)->toBe(1);

    // 3. Verify PipelineError quarantine & attribute redaction
    $errors = PipelineError::where('pipeline_id', $runId)->get();
    expect($errors)->toHaveCount(1)
        ->and($errors[0]->record_index)->toBe(1)
        ->and($errors[0]->raw_payload['email'])->toBe('invalid-email')
        ->and($errors[0]->raw_payload['password'])->toBe('[REDACTED]'); // Sensitive attribute redacted!

    // 4. Verify getQuarantinedRecords from repository
    $repo = app(PipelineRepositoryInterface::class);
    $quarantined = $repo->getQuarantinedRecords($runId);
    expect($quarantined)->toHaveCount(1)
        ->and($quarantined[1]['email'])->toBe('invalid-email');
});
