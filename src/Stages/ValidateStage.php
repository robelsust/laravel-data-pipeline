<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Stages;

use Robel\DataPipeline\Contracts\ChunkStageInterface;
use Robel\DataPipeline\RecordChunk;
use Robel\DataPipeline\Validation\BatchValidator;

final readonly class ValidateStage implements ChunkStageInterface
{
    private BatchValidator $validator;

    /**
     * @param  array<string, mixed>  $rules
     * @param  array<string, string>  $messages
     * @param  array<string, string>  $customAttributes
     */
    public function __construct(
        array $rules,
        array $messages = [],
        array $customAttributes = [],
    ) {
        $this->validator = new BatchValidator($rules, $messages, $customAttributes);
    }

    public function handleChunk(RecordChunk $chunk): RecordChunk
    {
        $this->validator->validateChunk($chunk);

        return $chunk;
    }
}
