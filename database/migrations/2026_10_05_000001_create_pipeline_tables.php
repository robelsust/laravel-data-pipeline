<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function getConnection(): ?string
    {
        return config('data-pipeline.connection');
    }

    public function up(): void
    {
        // 1. pipeline_runs
        Schema::connection($this->getConnection())->create('pipeline_runs', function (Blueprint $table) {
            $table->uuid('id')->primary();
            $table->string('name')->nullable();
            $table->string('status', 32)->index(); // CREATED, RUNNING, COMPLETED, PARTIAL_SUCCESS, FAILED, PAUSED
            $table->string('source_type');
            $table->string('source_fingerprint')->nullable();
            $table->unsignedBigInteger('total_records')->nullable();
            $table->unsignedBigInteger('processed_records')->default(0);
            $table->unsignedBigInteger('successful_records')->default(0);
            $table->unsignedBigInteger('failed_records')->default(0);
            $table->json('metadata')->nullable();
            $table->timestamp('started_at')->nullable();
            $table->timestamp('completed_at')->nullable();
            $table->timestamps();
        });

        // 2. pipeline_chunks
        Schema::connection($this->getConnection())->create('pipeline_chunks', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->uuid('pipeline_id')->index();
            $table->unsignedInteger('sequence');
            $table->string('status', 32); // PENDING, PROCESSING, COMPLETED, FAILED
            $table->unsignedInteger('records_count')->default(0);
            $table->unsignedInteger('successful_count')->default(0);
            $table->unsignedInteger('failed_count')->default(0);
            $table->unsignedBigInteger('last_record_offset')->nullable();
            $table->string('last_record_identifier')->nullable();
            $table->timestamp('started_at')->nullable();
            $table->timestamp('completed_at')->nullable();

            $table->unique(['pipeline_id', 'sequence']);
            $table->foreign('pipeline_id')->references('id')->on('pipeline_runs')->cascadeOnDelete();
        });

        // 3. pipeline_errors (Quarantine table)
        Schema::connection($this->getConnection())->create('pipeline_errors', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->uuid('pipeline_id')->index();
            $table->unsignedBigInteger('chunk_id')->nullable()->index();
            $table->unsignedBigInteger('record_index');
            $table->string('record_identifier')->nullable()->index();
            $table->json('raw_payload')->nullable();
            $table->json('validation_errors')->nullable();
            $table->text('exception_message')->nullable();
            $table->timestamp('failed_at')->useCurrent();

            $table->foreign('pipeline_id')->references('id')->on('pipeline_runs')->cascadeOnDelete();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection($this->getConnection())->dropIfExists('pipeline_errors');
        Schema::connection($this->getConnection())->dropIfExists('pipeline_chunks');
        Schema::connection($this->getConnection())->dropIfExists('pipeline_runs');
    }
};
