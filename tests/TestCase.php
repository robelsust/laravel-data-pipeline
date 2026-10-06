<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Tests;

use Orchestra\Testbench\TestCase as Orchestra;
use Robel\DataPipeline\DataPipelineServiceProvider;

abstract class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [
            DataPipelineServiceProvider::class,
        ];
    }

    protected function defineDatabaseMigrations(): void
    {
        if (getenv('PIPELINE_INTEGRATION') === '1') {
            \Orchestra\Testbench\load_migration_paths($this->app, [__DIR__.'/../database/migrations', __DIR__.'/Fixtures/migrations']);

            return;
        }
        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');
        $this->loadMigrationsFrom(__DIR__.'/Fixtures/migrations');
    }

    protected function getEnvironmentSetUp($app): void
    {
        $app['config']->set('database.default', 'sqlite');
        $app['config']->set('cache.default', 'array');
        $app['config']->set('queue.default', 'sync');
        $app['config']->set('queue.batching.database', 'sqlite');
        $app['config']->set('queue.failed.database', 'sqlite');
        $app['config']->set('data-pipeline.redacted_attributes', []);
        $app['config']->set('database.connections.sqlite', ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => '']);
        if (getenv('PIPELINE_INTEGRATION') === '1') {
            $app['config']->set('database.default', 'pgsql');
            $app['config']->set('database.connections.pgsql', [
                'driver' => 'pgsql',
                'host' => getenv('PIPELINE_PG_HOST') ?: '127.0.0.1',
                'port' => getenv('PIPELINE_PG_PORT') ?: '5432',
                'database' => 'pipeline_test',
                'username' => 'pipeline',
                'password' => 'pipeline-test',
                'charset' => 'utf8',
                'prefix' => '',
                'search_path' => 'public',
                'sslmode' => 'prefer',
            ]);
            $app['config']->set('database.redis.client', 'predis');
            $app['config']->set('database.redis.default', [
                'host' => getenv('PIPELINE_REDIS_HOST') ?: '127.0.0.1',
                'port' => getenv('PIPELINE_REDIS_PORT') ?: '6379',
                'database' => 0,
            ]);
            $app['config']->set('cache.stores.redis', ['driver' => 'redis', 'connection' => 'default', 'lock_connection' => 'default']);
        }
        $app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);
    }
}
