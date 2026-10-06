<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Commands;

use Illuminate\Console\Command;
use Robel\DataPipeline\Models\PipelineRun;

class PipelineListCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'pipeline:list {--status= : Filter pipelines by status (e.g. COMPLETED, FAILED, PARTIAL_SUCCESS)} {--limit=20 : Number of pipelines to show}';

    /**
     * The console command description.
     */
    protected $description = 'List recent data pipeline runs and their current status';

    public function handle(): int
    {
        $limit = (int) $this->option('limit');
        $query = PipelineRun::latest('started_at');

        if ($status = $this->option('status')) {
            $query->where('status', strtoupper($status));
        }

        $runs = $query->take($limit)->get();

        if ($runs->isEmpty()) {
            $this->info('No data pipeline runs found.');

            return self::SUCCESS;
        }

        $headers = ['ID', 'Name', 'Status', 'Source', 'Total', 'Processed', 'Success', 'Failed', 'Progress', 'Started'];
        $rows = [];

        foreach ($runs as $run) {
            $statusColor = match ($run->status) {
                'COMPLETED' => 'info',
                'PARTIAL_SUCCESS' => 'comment',
                'RUNNING' => 'question',
                'FAILED' => 'error',
                default => 'line',
            };

            $rows[] = [
                substr($run->id, 0, 8).'...',
                $run->name ?? 'Untitled',
                $run->status,
                $run->source_type,
                $run->total_records ?? 'Streaming',
                $run->processed_records,
                $run->successful_records,
                $run->failed_records,
                $run->progressPercentage().'%',
                $run->started_at?->diffForHumans() ?? 'N/A',
            ];
        }

        $this->table($headers, $rows);

        return self::SUCCESS;
    }
}
