<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Commands;

use Illuminate\Console\Command;
use Robel\DataPipeline\Contracts\PipelineDefinitionInterface;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineError;
use Robel\DataPipeline\Models\PipelineRun;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;
use Throwable;

class PipelineRetryFailedCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'pipeline:retry-failed
                            {id : The UUID of the pipeline run whose failed records should be retried}
                            {--class= : Pipeline definition class name implementing PipelineDefinitionInterface}
                            {--definition= : Registered pipeline definition name}';

    /**
     * The console command description.
     */
    protected $description = 'Surgically retry only the quarantined failed records of a pipeline run';

    public function handle(PipelineRepositoryInterface $repository): int
    {
        $id = $this->argument('id');
        $run = PipelineRun::find($id);

        if ($run === null) {
            $this->error("Pipeline run [{$id}] not found.");

            return self::FAILURE;
        }

        $errorCount = PipelineError::where('pipeline_id', $id)->count();

        if ($errorCount === 0) {
            $this->info("Pipeline [{$id}] has no quarantined failed records.");

            return self::SUCCESS;
        }

        $pipeline = null;
        $classOption = $this->option('class');
        $definitionOption = $this->option('definition');

        if (! empty($classOption)) {
            if (! class_exists($classOption)) {
                $this->error("Pipeline definition class [{$classOption}] does not exist.");

                return self::FAILURE;
            }
            $pipelineInstance = app($classOption);
            if ($pipelineInstance instanceof PipelineDefinitionInterface) {
                $pipeline = $pipelineInstance->pipeline();
            }
        } elseif (! empty($definitionOption)) {
            $pipeline = DataPipeline::resolveDefinition($definitionOption);
        } elseif (! empty($run->name)) {
            $pipeline = DataPipeline::resolveDefinition($run->name);
        }

        if ($pipeline === null) {
            $this->error("Cannot retry failed records for pipeline [{$id}]: no pipeline definition available. Please specify --class=<class> or register definition [{$run->name}].");

            return self::FAILURE;
        }

        $pipelineName = $run->name ?? $id;
        $this->warn("Found {$errorCount} quarantined record(s) for Pipeline [{$pipelineName}].");
        $this->line('Executing surgical retry for quarantined failed records...');

        try {
            $stats = DataPipeline::retryFailedRecords($id, $pipeline);

            $this->info("Surgical retry completed for Pipeline [{$pipelineName}]!");
            $this->table(
                ['Metric', 'Count'],
                [
                    ['Total Retried', $stats['retried']],
                    ['Recovered & Persisted', $stats['recovered']],
                    ['Remaining in Quarantine', $stats['remaining_failed']],
                ]
            );

            return self::SUCCESS;
        } catch (Throwable $e) {
            $this->error("Surgical retry failed: {$e->getMessage()}");

            return self::FAILURE;
        }
    }
}
