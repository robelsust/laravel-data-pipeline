<?php

declare(strict_types=1);

namespace Robel\DataPipeline;

use Illuminate\Support\ServiceProvider;
use Robel\DataPipeline\Commands\PipelineBenchmarkCommand;
use Robel\DataPipeline\Commands\PipelineListCommand;
use Robel\DataPipeline\Commands\PipelineResumeCommand;
use Robel\DataPipeline\Commands\PipelineRetryFailedCommand;
use Robel\DataPipeline\Commands\PipelineShowCommand;
use Robel\DataPipeline\Commands\PipelineStressTestCommand;
use Robel\DataPipeline\Repositories\DatabasePipelineRepository;
use Robel\DataPipeline\Repositories\PipelineRepositoryInterface;

final class DataPipelineServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__.'/../config/data-pipeline.php',
            'data-pipeline'
        );

        $this->app->bind(PipelineRepositoryInterface::class, function () {
            $connection = config('data-pipeline.connection');

            return new DatabasePipelineRepository($connection);
        });

        $this->app->singleton('data-pipeline', function () {
            return new class
            {
                public function from(mixed $source): DataPipeline
                {
                    return DataPipeline::from($source);
                }

                public function register(string $name, \Closure|string $definition): void
                {
                    DataPipeline::register($name, $definition);
                }

                public function resolveDefinition(string $name): ?DataPipeline
                {
                    return DataPipeline::resolveDefinition($name);
                }

                public function resumeRun(string $runId, ?DataPipeline $pipeline = null): Results\PipelineResult
                {
                    return DataPipeline::resumeRun($runId, $pipeline);
                }

                public function retryFailedRecords(string $runId, ?DataPipeline $pipeline = null): array
                {
                    return DataPipeline::retryFailedRecords($runId, $pipeline);
                }
            };
        });
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        // 1. Publish Configuration
        $this->publishes([
            __DIR__.'/../config/data-pipeline.php' => config_path('data-pipeline.php'),
        ], 'data-pipeline-config');

        // 2. Load and Publish Database Migrations
        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');

        $this->publishes([
            __DIR__.'/../database/migrations/' => database_path('migrations'),
        ], 'data-pipeline-migrations');

        // 3. Register Console Commands
        if ($this->app->runningInConsole()) {
            $this->commands([
                PipelineListCommand::class,
                PipelineShowCommand::class,
                PipelineResumeCommand::class,
                PipelineRetryFailedCommand::class,
                PipelineBenchmarkCommand::class,
                PipelineStressTestCommand::class,
            ]);
        }
    }
}
