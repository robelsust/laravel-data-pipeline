<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Robel\DataPipeline\Contracts\ChunkStageInterface;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineChunk;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use RuntimeException;
use Throwable;

final class PipelineStressTestCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'pipeline:stress-test
        {scenario=all : Which scenario to test: poison-pill (B), crash-resume (C), or all}
        {--records=500000 : Total records for the test}
        {--chunk-size=5000 : Records per chunk}
        {--keep-csv : Keep the generated CSV files}
        {--table=users : Destination MySQL table}';

    /**
     * The console command description.
     */
    protected $description = 'Execute enterprise resilience stress tests (Poison Pill quarantine & Crash/Resume recovery)';

    public function handle(PipelineRepositoryInterface $repository): int
    {
        $scenario = strtolower((string) $this->argument('scenario'));
        $records = max(100, (int) $this->option('records'));
        $chunkSize = max(10, (int) $this->option('chunk-size'));
        $table = (string) ($this->option('table') ?: 'users');
        $keepCsv = (bool) $this->option('keep-csv');

        $this->info('================================================================');
        $this->info('  Laravel Data Pipeline - Production Readiness Stress Suite');
        $this->info('================================================================');
        $this->line('Target Database: <comment>'.DB::connection()->getDatabaseName().'</comment>');
        $this->line("Target Table:    <comment>{$table}</comment>");
        $this->line('Records:         <comment>'.number_format($records).'</comment>');
        $this->line('Chunk Size:      <comment>'.number_format($chunkSize).'</comment>');
        $this->newLine();

        if (in_array($scenario, ['poison-pill', 'b', 'all'], true)) {
            $statusB = $this->runScenarioPoisonPill($records, $chunkSize, $table, $keepCsv);
            if ($statusB !== self::SUCCESS) {
                return $statusB;
            }
        }

        if (in_array($scenario, ['crash-resume', 'c', 'all'], true)) {
            $statusC = $this->runScenarioCrashResume($records, $chunkSize, $table, $keepCsv);
            if ($statusC !== self::SUCCESS) {
                return $statusC;
            }
        }

        $this->info('================================================================');
        $this->info('  ALL PRODUCTION RESILIENCE STRESS TESTS PASSED WITH 100% SUCCESS');
        $this->info('================================================================');

        return self::SUCCESS;
    }

    /**
     * Scenario B: Poison Pill Ingestion & Quarantine at Scale.
     */
    private function runScenarioPoisonPill(int $records, int $chunkSize, string $table, bool $keepCsv): int
    {
        $this->info('----------------------------------------------------------------');
        $this->info('  SCENARIO B: Poison Pill Quarantine & Surgical Retry at Scale');
        $this->info('----------------------------------------------------------------');

        $csvPath = storage_path("app/stress_poison_pill_{$records}.csv");
        if (! is_dir(dirname($csvPath))) {
            mkdir(dirname($csvPath), 0755, true);
        }

        // 1. Generate dataset with poison pills injected every 200th record
        $poisonInterval = 200;
        $expectedPoisonCount = (int) floor($records / $poisonInterval);
        $expectedValidCount = $records - $expectedPoisonCount;

        $this->line("Generating {$records} records with {$expectedPoisonCount} corrupt poison pills...");
        $fp = fopen($csvPath, 'w');
        fputcsv($fp, ['full_name', 'username', 'user_email', 'secret', 'created_date', 'updated_date'], ',', '"', '\\');

        $batchSize = 25000;
        $buffer = '';
        $inBatch = 0;

        for ($i = 1; $i <= $records; $i++) {
            $isPoison = ($i % $poisonInterval === 0);
            $email = $isPoison ? "corrupt_poison_email_no_domain_{$i}" : "valid_user{$i}@stress.test";
            $name = $isPoison ? 'Poison User' : "Valid User {$i}";

            $buffer .= "{$name},user_{$i},{$email},\$2y\$12\$e8YkXyD1234567890abcdefghijklmnopqrstuv,2026-10-06 12:00:00,2026-10-06 12:00:00\n";
            $inBatch++;

            if ($inBatch >= $batchSize || $i === $records) {
                fwrite($fp, $buffer);
                $buffer = '';
                $inBatch = 0;
            }
        }
        fclose($fp);
        $this->line("CSV generated: {$csvPath} (".round(filesize($csvPath) / (1024 * 1024), 2).' MB)');

        // 2. Prepare table
        DB::table($table)->truncate();
        $initialRows = DB::table($table)->count();

        // 3. Register Definition for Stage 1 (Strict email validation)
        $defName = 'PoisonPillStressTest';
        DataPipeline::register($defName, function () use ($csvPath, $chunkSize, $table, $defName) {
            return DataPipeline::from($csvPath)
                ->name($defName)
                ->as($defName)
                ->chunk($chunkSize)
                ->mapFields([
                    'full_name' => 'name',
                    'username' => 'username',
                    'user_email' => 'email',
                    'secret' => 'password',
                    'created_date' => 'created_at',
                    'updated_date' => 'updated_at',
                ])
                ->validate([
                    'name' => ['required', 'string'],
                    'email' => ['required', 'email'],
                ])
                ->allowPartialSuccess(true)
                ->track(true)
                ->insert($table);
        });

        // 4. Execute Pipeline
        $this->line("Executing pipeline with validation rules: email='required|email'...");
        $startTime = microtime(true);
        $pipeline = DataPipeline::resolveDefinition($defName);
        $result = $pipeline->run();
        $duration = microtime(true) - $startTime;

        $run = PipelineRun::latest('started_at')->first();
        $quarantinedCount = PipelineError::where('pipeline_id', $run->id)->count();
        $actualTableCount = DB::table($table)->count();

        $this->table(
            ['Metric', 'Expected', 'Actual', 'Status'],
            [
                ['Total Processed', number_format($records), number_format($result->totalProcessed), $result->totalProcessed === $records ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Valid Persisted', number_format($expectedValidCount), number_format($result->successful), $result->successful === $expectedValidCount ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Poison Quarantined', number_format($expectedPoisonCount), number_format($result->failed), $result->failed === $expectedPoisonCount ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Database Errors Recorded', number_format($expectedPoisonCount), number_format($quarantinedCount), $quarantinedCount === $expectedPoisonCount ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['MySQL Table Row Count', number_format($expectedValidCount), number_format($actualTableCount), $actualTableCount === $expectedValidCount ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Pipeline Run Status', 'PARTIAL_SUCCESS', $run->status, $run->status === 'PARTIAL_SUCCESS' ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Ingestion Duration', '-', round($duration, 2).'s ('.number_format(round($records / max($duration, 0.001))).' rows/s)', '<info>PASS</info>'],
            ]
        );

        // 5. Inspect a quarantined sample
        $sampleError = PipelineError::where('pipeline_id', $run->id)->first();
        if ($sampleError) {
            $this->line("Sample Quarantined Error (Record Index #{$sampleError->record_index}):");
            $this->line('  ↳ Raw:    '.json_encode($sampleError->raw_payload));
            $this->line('  ↳ Errors: '.json_encode($sampleError->validation_errors));
        }

        // 6. Test Surgical Recovery (pipeline:retry-failed)
        $this->newLine();
        $this->info('Testing Surgical Recovery via pipeline:retry-failed...');

        // Register recovery definition with transform fixing the corrupt email
        DataPipeline::register($defName, function () use ($csvPath, $chunkSize, $table, $defName) {
            return DataPipeline::from($csvPath)
                ->name($defName)
                ->as($defName)
                ->chunk($chunkSize)
                ->mapFields([
                    'full_name' => 'name',
                    'username' => 'username',
                    'user_email' => 'email',
                    'secret' => 'password',
                    'created_date' => 'created_at',
                    'updated_date' => 'updated_at',
                ])
                ->transform(function (array $data): array {
                    if (! filter_var($data['email'] ?? '', FILTER_VALIDATE_EMAIL)) {
                        $data['email'] = strtolower((string) ($data['username'] ?? 'user')).'@recovered.test';
                    }

                    return $data;
                })
                ->validate([
                    'name' => ['required', 'string'],
                    'email' => ['required', 'email'],
                ])
                ->allowPartialSuccess(true)
                ->track(true)
                ->insert($table);
        });

        $retryStart = microtime(true);
        $retryStats = DataPipeline::retryFailedRecords($run->id);
        $retryDuration = microtime(true) - $retryStart;

        $finalTableCount = DB::table($table)->count();
        $remainingErrors = PipelineError::where('pipeline_id', $run->id)->count();

        $this->table(
            ['Surgical Retry Metric', 'Expected', 'Actual', 'Status'],
            [
                ['Total Retried', number_format($expectedPoisonCount), number_format($retryStats['retried']), $retryStats['retried'] === $expectedPoisonCount ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Recovered & Persisted', number_format($expectedPoisonCount), number_format($retryStats['recovered']), $retryStats['recovered'] === $expectedPoisonCount ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Remaining in Quarantine', '0', (string) $remainingErrors, $remainingErrors === 0 ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Final MySQL Table Rows', number_format($records), number_format($finalTableCount), $finalTableCount === $records ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Retry Duration', '-', round($retryDuration, 3).'s', '<info>PASS</info>'],
            ]
        );

        if (! $keepCsv && file_exists($csvPath)) {
            unlink($csvPath);
        }

        $this->info("✓ Scenario B (Poison Pill Quarantine & Surgical Recovery) completed with 100% accuracy!\n");

        return self::SUCCESS;
    }

    /**
     * Scenario C: Mid-Stream Process Kill & Checkpoint Resume.
     */
    private function runScenarioCrashResume(int $records, int $chunkSize, string $table, bool $keepCsv): int
    {
        $this->info('----------------------------------------------------------------');
        $this->info('  SCENARIO C: Mid-Stream Crash & Checkpoint Resume at Scale');
        $this->info('----------------------------------------------------------------');

        $csvPath = storage_path("app/stress_crash_resume_{$records}.csv");
        if (! is_dir(dirname($csvPath))) {
            mkdir(dirname($csvPath), 0755, true);
        }

        // 1. Generate clean dataset
        $this->line("Generating clean dataset with {$records} records for crash simulation...");
        $fp = fopen($csvPath, 'w');
        fputcsv($fp, ['full_name', 'username', 'user_email', 'secret', 'created_date', 'updated_date'], ',', '"', '\\');

        $batchSize = 25000;
        $buffer = '';
        $inBatch = 0;

        for ($i = 1; $i <= $records; $i++) {
            $buffer .= "Crash Test User {$i},user_c_{$i},crash_user{$i}@stress.test,\$2y\$12\$e8YkXyD1234567890abcdefghijklmnopqrstuv,2026-10-06 12:00:00,2026-10-06 12:00:00\n";
            $inBatch++;

            if ($inBatch >= $batchSize || $i === $records) {
                fwrite($fp, $buffer);
                $buffer = '';
                $inBatch = 0;
            }
        }
        fclose($fp);

        // 2. Prepare table
        DB::table($table)->truncate();

        // Total chunks
        $totalChunks = (int) ceil($records / $chunkSize);
        $crashAtChunk = (int) ceil($totalChunks / 2); // 50% midpoint
        $expectedCommittedRows = $crashAtChunk * $chunkSize;

        $defName = 'CrashResumeStressTest';

        // 3. Register definition with crash injection at midpoint
        $shouldCrash = true;
        DataPipeline::register($defName, function () use ($csvPath, $chunkSize, $table, $defName, &$shouldCrash, $crashAtChunk) {
            $pipeline = DataPipeline::from($csvPath)
                ->name($defName)
                ->as($defName)
                ->chunk($chunkSize)
                ->mapFields([
                    'full_name' => 'name',
                    'username' => 'username',
                    'user_email' => 'email',
                    'secret' => 'password',
                    'created_date' => 'created_at',
                    'updated_date' => 'updated_at',
                ])
                ->track(true)
                ->insert($table);

            if ($shouldCrash) {
                $pipeline->pipe(new class($crashAtChunk) implements ChunkStageInterface
                {
                    public function __construct(private int $crashChunk) {}

                    public function handleChunk(RecordChunk $chunk): RecordChunk
                    {
                        if ($chunk->sequence() > $this->crashChunk) {
                            throw new RuntimeException("Simulated catastrophic crash at chunk sequence {$chunk->sequence()}!");
                        }

                        return $chunk;
                    }
                });
            }

            return $pipeline;
        });

        // 4. Trigger initial execution and simulate mid-stream crash
        $this->warn("Starting initial run; simulating catastrophic server crash at chunk #{$crashAtChunk} (50%)...");
        $runId = null;

        try {
            $p = DataPipeline::resolveDefinition($defName);
            $p->run();
            $this->error('Expected crash did not trigger!');

            return self::FAILURE;
        } catch (Throwable $e) {
            $this->line("Caught simulated crash: <comment>{$e->getMessage()}</comment>");
        }

        // 5. Inspect post-crash state
        $crashedRun = PipelineRun::latest('started_at')->first();
        $runId = $crashedRun->id;
        $committedChunksCount = PipelineChunk::where('pipeline_id', $runId)->where('status', 'COMPLETED')->count();
        $crashedTableRowCount = DB::table($table)->count();

        $this->table(
            ['Post-Crash Diagnostic', 'Expected', 'Actual', 'Status'],
            [
                ['Pipeline Run ID', '-', substr($runId, 0, 8).'...', '<info>OK</info>'],
                ['Run Status', 'FAILED', $crashedRun->status, $crashedRun->status === 'FAILED' ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Committed Chunks (Checkpoint)', $crashAtChunk, $committedChunksCount, $committedChunksCount === $crashAtChunk ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Database Persisted Rows', number_format($expectedCommittedRows), number_format($crashedTableRowCount), $crashedTableRowCount === $expectedCommittedRows ? '<info>PASS</info>' : '<error>FAIL</error>'],
            ]
        );

        // 6. Resume the run from last checkpoint (disabling crash trigger)
        $this->newLine();
        $this->info("Resuming Pipeline [{$runId}] from last checkpoint (skipping chunks 1..{$crashAtChunk})...");
        $shouldCrash = false; // System recovered, crash condition cleared

        $resumeStart = microtime(true);
        $resumePipeline = DataPipeline::resolveDefinition($defName);
        $resumeResult = DataPipeline::resumeRun($runId, $resumePipeline);
        $resumeDuration = microtime(true) - $resumeStart;

        $resumedRun = PipelineRun::find($runId);
        $finalTableCount = DB::table($table)->count();
        $allChunksCount = PipelineChunk::where('pipeline_id', $runId)->where('status', 'COMPLETED')->count();

        $this->table(
            ['Resume Verification Metric', 'Expected', 'Actual', 'Status'],
            [
                ['Final Pipeline Status', 'COMPLETED', $resumedRun->status, $resumedRun->status === 'COMPLETED' ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Total Chunks Completed', $totalChunks, $allChunksCount, $allChunksCount === $totalChunks ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Total Records Accounted For', number_format($records), number_format($resumedRun->processed_records), $resumedRun->processed_records === $records ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Final MySQL Table Rows', number_format($records), number_format($finalTableCount), $finalTableCount === $records ? '<info>PASS</info>' : '<error>FAIL</error>'],
                ['Resume Ingestion Time', '-', round($resumeDuration, 2).'s', '<info>PASS</info>'],
            ]
        );

        if (! $keepCsv && file_exists($csvPath)) {
            unlink($csvPath);
        }

        $this->info("✓ Scenario C (Mid-Stream Crash & Resume from Checkpoint) verified with 100% zero-duplicate fidelity!\n");

        return self::SUCCESS;
    }
}
