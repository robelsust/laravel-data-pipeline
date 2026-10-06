<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Commands;

use Illuminate\Console\Command;
use Robel\DataPipeline\Contracts\PipelineDefinitionInterface;
use Robel\DataPipeline\DataPipeline;
use Robel\DataPipeline\Models\PipelineRun;
use Throwable;

class PipelineResumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'pipeline:resume
                            {id : The UUID of the pipeline run to resume}
                            {--class= : Pipeline definition class name implementing PipelineDefinitionInterface}
                            {--definition= : Registered pipeline definition name}';

    /**
     * The console command description.
     */
    protected $description = 'Resume an interrupted, paused, or failed data pipeline run from its last checkpoint';

    public function handle(): int
    {
        $id = $this->argument('id');
        $run = PipelineRun::find($id);

        if ($run === null) {
            $this->error("Pipeline run [{$id}] not found.");

            return self::FAILURE;
        }

        if ($run->status === 'COMPLETED') {
            $this->warn("Pipeline [{$id}] has already completed successfully.");

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
            $this->error("Cannot resume pipeline [{$id}]: no pipeline definition available. Please specify --class=<class> or register definition [{$run->name}].");

            return self::FAILURE;
        }

        $pipelineName = $run->name ?? $id;
        $this->info("Resuming execution for Pipeline [{$pipelineName}] (Run ID: {$id})...");

        try {
            $result = DataPipeline::resumeRun($id, $pipeline);

            $this->info("Pipeline [{$pipelineName}] resumed successfully!");
            $this->table(
                ['Metric', 'Value'],
                [
                    ['Total Processed', $result->totalProcessed],
                    ['Successful', $result->successful],
                    ['Failed', $result->failed],
                    ['Status', $result->isSuccessful() ? 'COMPLETED' : 'PARTIAL_SUCCESS'],
                    ['Duration', "{$result->durationInSeconds}s"],
                ]
            );

            return self::SUCCESS;
        } catch (Throwable $e) {
            $this->error("Resumption failed: {$e->getMessage()}");

            return self::FAILURE;
        }
    }
}
