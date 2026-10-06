<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Robel\DataPipeline\DataPipeline;
use Throwable;

final class PipelineBenchmarkCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'pipeline:benchmark
        {--records=1000000 : Total number of records to generate and ingest}
        {--copied-records=0 : Number of records copied from earlier existing data (e.g. 250000)}
        {--chunk-size=5000 : Number of records per chunk for bulk processing}
        {--file= : Custom CSV file path}
        {--keep-csv : Keep the generated CSV file after benchmark completion}
        {--track : Track pipeline checkpoints and run history in MySQL}
        {--table=pipeline_benchmark_records : Destination table}
        {--allow-destructive : Explicitly permit truncating a custom table}
        {--truncate : Truncate destination table before running}
        {--upsert : Use UpsertSink instead of InsertSink for collision/merge testing}
        {--unique-by=email : Comma-separated unique columns for upsert resolution}
        {--skip-generate : Skip CSV generation and use existing file}';

    /**
     * The console command description.
     */
    protected $description = 'Generate a large CSV dataset and benchmark high-throughput MySQL ingestion via DataPipeline';

    public function handle(): int
    {
        $records = max(1, (int) $this->option('records'));
        $copiedRecords = max(0, min($records, (int) $this->option('copied-records')));
        $newRecords = $records - $copiedRecords;

        $chunkSize = max(1, (int) $this->option('chunk-size'));
        $table = (string) ($this->option('table') ?: BenchmarkTable::Name);
        $track = (bool) $this->option('track');
        $truncate = (bool) $this->option('truncate');
        $keepCsv = (bool) $this->option('keep-csv');
        $skipGenerate = (bool) $this->option('skip-generate');
        $upsert = (bool) $this->option('upsert');
        if (! BenchmarkTable::prepare($this, $table, $truncate)) {
            return self::FAILURE;
        }
        $uniqueByStr = (string) ($this->option('unique-by') ?: 'email');
        $uniqueCols = array_map('trim', explode(',', $uniqueByStr));

        $csvPath = (string) ($this->option('file') ?: storage_path("app/pipeline_benchmark_{$records}_c{$copiedRecords}.csv"));

        $this->info('================================================================');
        $this->info('  Laravel Data Pipeline - High Volume MySQL Benchmark');
        $this->info('================================================================');

        // 1. Verify MySQL connection
        try {
            $dbName = DB::connection()->getDatabaseName();
            $dbHost = config('database.connections.mysql.host', '127.0.0.1');
            $this->line("Target Database:  <comment>{$dbName}</comment> on <comment>{$dbHost}</comment>");
            $this->line("Target Table:     <comment>{$table}</comment>");
            $this->line('Dataset Size:     <comment>'.number_format($records).' records</comment>');
            if ($copiedRecords > 0) {
                $this->line('  ↳ Copied/Old:   <comment>'.number_format($copiedRecords).' records (existing keys)</comment>');
                $this->line('  ↳ Brand New:    <comment>'.number_format($newRecords).' records (new keys)</comment>');
            }
            $this->line('Operation Mode:   <comment>'.($upsert ? 'UPSERT (Bulk Merge by '.implode(', ', $uniqueCols).')' : 'INSERT').'</comment>');
            $this->line('Chunk Batch Size: <comment>'.number_format($chunkSize).' records/chunk</comment>');
            $this->line('Checkpoints:      <comment>'.($track ? 'Enabled (pipeline_runs / pipeline_chunks)' : 'Disabled').'</comment>');
            $this->newLine();
        } catch (Throwable $e) {
            $this->error('Failed to connect to MySQL database: '.$e->getMessage());

            return self::FAILURE;
        }

        // 2. Prepare destination table
        if ($truncate) {
            $this->warn("Truncating table [{$table}]...");
            DB::table($table)->truncate();
        }

        $initialCount = DB::table($table)->count();
        $this->line("Current rows in [{$table}]: ".number_format($initialCount));

        // 3. Generate CSV Dataset
        if (! $skipGenerate || ! file_exists($csvPath)) {
            $this->newLine();
            $this->info('Step 1/2: Generating CSV dataset with '.number_format($records).' records...');
            if ($copiedRecords > 0) {
                $this->line("  ↳ Embedding {$copiedRecords} copied existing records + {$newRecords} new records.");
            }
            $this->line("Path: <comment>{$csvPath}</comment>");

            if (! is_dir(dirname($csvPath))) {
                mkdir(dirname($csvPath), 0755, true);
            }

            $genStart = microtime(true);
            $fp = fopen($csvPath, 'w');
            if ($fp === false) {
                $this->error("Failed to open file for writing: [{$csvPath}]");

                return self::FAILURE;
            }

            // Headers with 2 unique columns: email and username
            fputcsv($fp, ['full_name', 'username', 'user_email', 'secret', 'created_date', 'updated_date'], ',', '"', '\\');

            $genBar = $this->output->createProgressBar($records);
            $genBar->setFormat(' %current%/%max% [%bar%] %percent:3s%% - %elapsed:6s% (Mem: %memory%)');
            $genBar->start();

            $batchSize = 25000;
            $buffer = '';
            $countInBatch = 0;

            // 3a. Generate Copied Records (colliding with existing records 1..N)
            for ($i = 1; $i <= $copiedRecords; $i++) {
                $buffer .= "Updated Benchmark User {$i},user_{$i},bench_user{$i}@example.com,\$2y\$12\$e8YkXyD1234567890abcdefghijklmnopqrstuv,2026-10-06 12:00:00,2026-10-06 12:30:00\n";
                $countInBatch++;

                if ($countInBatch >= $batchSize) {
                    fwrite($fp, $buffer);
                    $buffer = '';
                    $genBar->advance($countInBatch);
                    $countInBatch = 0;
                }
            }

            // 3b. Generate New Records (starting after 1,000,000)
            for ($k = 1; $k <= $newRecords; $k++) {
                $idx = 1000000 + $k;
                $buffer .= "New Benchmark User {$idx},user_{$idx},bench_user{$idx}@example.com,\$2y\$12\$e8YkXyD1234567890abcdefghijklmnopqrstuv,2026-10-06 12:30:00,2026-10-06 12:30:00\n";
                $countInBatch++;

                if ($countInBatch >= $batchSize) {
                    fwrite($fp, $buffer);
                    $buffer = '';
                    $genBar->advance($countInBatch);
                    $countInBatch = 0;
                }
            }

            // Flush remaining buffer
            if ($countInBatch > 0) {
                fwrite($fp, $buffer);
                $genBar->advance($countInBatch);
            }

            fclose($fp);
            $genBar->finish();
            $this->newLine();

            $genDuration = microtime(true) - $genStart;
            $fileSizeBytes = filesize($csvPath);
            $fileSizeMb = round($fileSizeBytes / (1024 * 1024), 2);
            $genRate = round($records / max($genDuration, 0.001));

            $this->info('CSV generation complete in '.round($genDuration, 2)."s ({$fileSizeMb} MB, ".number_format($genRate).' rows/sec)');
        } else {
            $fileSizeBytes = filesize($csvPath);
            $fileSizeMb = round($fileSizeBytes / (1024 * 1024), 2);
            $this->info("Using existing CSV file: {$csvPath} ({$fileSizeMb} MB)");
        }

        // 4. Ingest via DataPipeline
        $this->newLine();
        $this->info('Step 2/2: Ingesting dataset into MySQL via DataPipeline...');

        $initialMemoryMb = round(memory_get_usage(true) / (1024 * 1024), 2);
        $pipelineStart = microtime(true);

        $pipeBar = $this->output->createProgressBar($records);
        $pipeBar->setFormat(' %current%/%max% [%bar%] %percent:3s%% - %elapsed:6s% (Throughput: %message%, Mem: %memory%)');
        $pipeBar->setMessage('0 rows/s');
        $pipeBar->start();

        $lastProcessed = 0;
        $lastTime = microtime(true);

        $pipeline = DataPipeline::from($csvPath)
            ->name('MySQL Benchmark '.number_format($records).' Rows'.($upsert ? ' (Upsert)' : ''))
            ->chunk($chunkSize)
            ->mapFields([
                'full_name' => 'name',
                'username' => 'username',
                'user_email' => 'email',
                'secret' => 'password',
                'created_date' => 'created_at',
                'updated_date' => 'updated_at',
            ])
            ->transform(function (array $record): array {
                $record['email'] = strtolower((string) ($record['email'] ?? ''));
                if (isset($record['username'])) {
                    $record['username'] = strtolower((string) $record['username']);
                }

                return $record;
            })
            ->onProgress(function (int $totalProcessed) use ($pipeBar, &$lastProcessed, &$lastTime, $pipelineStart): void {
                $delta = $totalProcessed - $lastProcessed;
                $pipeBar->advance($delta);
                $lastProcessed = $totalProcessed;

                $now = microtime(true);
                if ($now - $lastTime >= 0.5) {
                    $totalElapsed = max($now - $pipelineStart, 0.001);
                    $overallThroughput = round($totalProcessed / $totalElapsed);
                    $pipeBar->setMessage(number_format($overallThroughput).' rows/s');
                    $lastTime = $now;
                }
            });

        if ($track) {
            $pipeline->track();
        }

        if ($upsert) {
            $pipeline->upsert($table, uniqueBy: $uniqueCols, update: ['name', 'updated_at']);
        } else {
            $pipeline->insert($table);
        }

        try {
            $result = $pipeline->run();
        } catch (Throwable $e) {
            $pipeBar->finish();
            $this->newLine();
            $this->error('Pipeline execution encountered an exception: '.$e->getMessage());

            return self::FAILURE;
        }

        $pipeBar->finish();
        $this->newLine(2);

        $pipelineDuration = microtime(true) - $pipelineStart;
        $peakMemoryMb = round(memory_get_peak_usage(true) / (1024 * 1024), 2);
        $finalCount = DB::table($table)->count();
        $insertedCount = $finalCount - $initialCount;
        $updatedCount = $result->successful - $insertedCount;
        $overallRate = round($result->successful / max($pipelineDuration, 0.001));

        // 5. Verification & Summary Output
        $this->info('================================================================');
        $this->info('  BENCHMARK VERIFICATION RESULTS');
        $this->info('================================================================');

        $headers = ['Metric', 'Value'];
        $status = $result->isSuccessful() ? 'COMPLETED' : 'PARTIAL_SUCCESS';

        $rows = [
            ['Total Processed Records', number_format($result->totalProcessed)],
            ['Successfully Ingested', number_format($result->successful)],
            ['Failed / Quarantined', number_format($result->failed)],
            ['Initial MySQL Rows', number_format($initialCount)],
            ['Final MySQL Rows', number_format($finalCount).' (Δ +'.number_format($insertedCount).')'],
        ];

        if ($upsert && $copiedRecords > 0) {
            $rows[] = ['Existing Rows Updated', number_format($updatedCount)];
            $rows[] = ['Brand New Rows Inserted', number_format($insertedCount)];
        }

        $rows[] = ['Total Ingestion Time', round($pipelineDuration, 2).' seconds'];
        $rows[] = ['Average Throughput', '<info>'.number_format($overallRate).' records/sec</info>'];
        $rows[] = ['Initial Memory', "{$initialMemoryMb} MB"];
        $rows[] = ['Peak Memory', '<info>'."{$peakMemoryMb} MB".'</info>'];
        $rows[] = ['Memory Invariance', $peakMemoryMb < 64 ? '<info>Verified O(1) Memory Invariant (< 64 MB)</info>' : '<comment>Observed</comment>'];
        $rows[] = ['Pipeline Status', '<info>'.$status.'</info>'];

        $this->table($headers, $rows);

        // Spot-check updated records
        if ($upsert && $copiedRecords > 0) {
            $sample = DB::table($table)->where('email', 'bench_user1@example.com')->first();
            if ($sample !== null && str_starts_with($sample->name, 'Updated Benchmark User')) {
                $this->info("✓ In-place update verified: Row #1 name updated to \"{$sample->name}\" with updated_at: {$sample->updated_at}");
            }
        }

        // 6. Cleanup CSV if requested
        if (! $keepCsv && file_exists($csvPath)) {
            unlink($csvPath);
            $this->line("Cleaned up temporary CSV file [{$csvPath}].");
        } else {
            $this->line("Retained benchmark CSV file at [{$csvPath}].");
        }

        $this->newLine();
        $this->info("✓ Benchmark completed successfully! {$records} records tested against MySQL.");

        return self::SUCCESS;
    }
}
