<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function getConnection(): ?string
    {
        return config('data-pipeline.connection');
    }

    public function up(): void
    {
        Schema::connection($this->getConnection())->table('pipeline_chunks', function (Blueprint $table): void {
            $table->uuid('lease_token')->nullable();
        });
        Schema::connection($this->getConnection())->create('pipeline_deduplication_claims', function (Blueprint $table): void {
            $table->id();
            $table->uuid('pipeline_id');
            $table->string('scope', 64);
            $table->string('signature', 32);
            $table->text('record_owner');
            $table->unique(['pipeline_id', 'scope', 'signature'], 'pipeline_dedup_claim_unique');
            $table->foreign('pipeline_id')->references('id')->on('pipeline_runs')->cascadeOnDelete();
        });
    }

    public function down(): void
    {
        Schema::connection($this->getConnection())->dropIfExists('pipeline_deduplication_claims');
        Schema::connection($this->getConnection())->table('pipeline_chunks', function (Blueprint $table): void {
            $table->dropColumn('lease_token');
        });
    }
};
