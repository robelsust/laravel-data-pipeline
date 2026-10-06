<?php

namespace Robel\DataPipeline\Tests\Feature;

use Robel\DataPipeline\PipelineManager;
use Robel\DataPipeline\Tests\TestCase;

class ExampleTest extends TestCase
{
    public function test_provider_registers_the_package_manager(): void
    {
        $this->assertInstanceOf(PipelineManager::class, $this->app->make('data-pipeline'));
    }
}
