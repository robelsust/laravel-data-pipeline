<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Commands;

use Illuminate\Console\Command;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;

class PipelineShowCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'pipeline:show {id : The UUID of the pipeline run}';

    /**
     * The console command description.
     */
    protected $description = 'Inspect detailed diagnostics and quarantined errors of a data pipeline run';

    public function handle(PipelineRepositoryInterface $repository): int
    {
        $id = $this->argument('id');
        $run = $repository->findRun($id);

        if ($run === null) {
            $this->error("Pipeline run [{$id}] not found.");

            return self::FAILURE;
        }

        $this->info("Pipeline Run: {$run->name} [{$run->id}]");
        $this->line("Status:    {$run->status}");
        $this->line("Source:    {$run->source_type}");
        $this->line("Progress:  {$run->processed_records} / ".($run->total_records ?? 'Unknown')." ({$run->progressPercentage()}%)");
        $this->line("Succeeded: {$run->successful_records}");
        $this->line("Failed:    {$run->failed_records}");
        $this->line("Started:   {$run->started_at}");
        $this->line('Completed: '.($run->completed_at ?? 'In Progress'));

        // Display Chunks Summary
        $chunks = $run->chunks()->orderBy('sequence')->get();
        if ($chunks->isNotEmpty()) {
            $this->newLine();
            $this->info("Chunks Breakdown ({$chunks->count()} chunks):");
            $chunkRows = [];
            foreach ($chunks as $chunk) {
                $chunkRows[] = [
                    $chunk->sequence,
                    $chunk->status,
                    $chunk->records_count,
                    $chunk->successful_count,
                    $chunk->failed_count,
                    $chunk->last_record_offset ?? '-',
                ];
            }
            $this->table(['Seq', 'Status', 'Records', 'Success', 'Failed', 'Last Offset'], $chunkRows);
        }

        // Display Quarantined Errors
        $errors = $run->errors()->take(10)->get();
        if ($errors->isNotEmpty()) {
            $this->newLine();
            $this->warn("Quarantined Errors (Showing first 10 of {$run->failed_records}):");
            $errorRows = [];
            foreach ($errors as $err) {
                $errorMessages = [];
                if (is_array($err->validation_errors)) {
                    foreach ($err->validation_errors as $field => $msgs) {
                        $errorMessages[] = "{$field}: ".implode(', ', (array) $msgs);
                    }
                }

                $errorRows[] = [
                    $err->record_index,
                    $err->record_identifier ?? '-',
                    implode('; ', $errorMessages) ?: ($err->exception_message ?? 'Unknown error'),
                ];
            }
            $this->table(['Row Index', 'Identifier', 'Errors / Exception'], $errorRows);
        }

        return self::SUCCESS;
    }
}
