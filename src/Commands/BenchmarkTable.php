<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Commands;

use Illuminate\Console\Command;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/** Keeps generated load-test data separate from application records. */
final class BenchmarkTable
{
    public const Name = 'pipeline_benchmark_records';

    public static function prepare(Command $command, string $table, bool $destructive): bool
    {
        if ($table !== self::Name && $destructive && ! $command->option('allow-destructive')) {
            $command->error('Custom tables require --allow-destructive before truncation.');

            return false;
        }
        if ($table === self::Name && ! Schema::hasTable($table)) {
            Schema::create($table, function (Blueprint $blueprint): void {
                $blueprint->id();
                $blueprint->string('name');
                $blueprint->string('username')->unique();
                $blueprint->string('email')->unique();
                $blueprint->string('password');
                $blueprint->timestamps();
            });
        }

        return true;
    }
}
