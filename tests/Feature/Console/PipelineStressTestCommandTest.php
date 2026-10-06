<?php

declare(strict_types=1);

use App\Models\User;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

it('executes pipeline:stress-test poison-pill scenario B and verifies quarantine and surgical recovery', function () {
    $this->artisan('pipeline:stress-test', [
        'scenario' => 'poison-pill',
        '--records' => 400,
        '--chunk-size' => 50,
    ])
        ->expectsOutputToContain('SCENARIO B: Poison Pill Quarantine & Surgical Retry at Scale')
        ->expectsOutputToContain('Poison Pill Quarantine & Surgical Recovery) completed with 100% accuracy')
        ->assertSuccessful();

    // 400 records with poison pills every 200 = 2 poison pills
    // After surgical retry, all 400 records recovered into User table
    expect(User::count())->toBe(400);

    $run = PipelineRun::latest('started_at')->first();
    expect(PipelineError::where('pipeline_id', $run->id)->count())->toBe(0);
});

it('executes pipeline:stress-test crash-resume scenario C and verifies zero-duplicate recovery', function () {
    $this->artisan('pipeline:stress-test', [
        'scenario' => 'crash-resume',
        '--records' => 100,
        '--chunk-size' => 20,
    ])
        ->expectsOutputToContain('SCENARIO C: Mid-Stream Crash & Checkpoint Resume at Scale')
        ->expectsOutputToContain('Mid-Stream Crash & Resume from Checkpoint) verified with 100% zero-duplicate fidelity')
        ->assertSuccessful();

    expect(User::count())->toBe(100);

    $run = PipelineRun::latest('started_at')->first();
    expect($run->status)->toBe('COMPLETED');
});
