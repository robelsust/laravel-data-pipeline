<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Exceptions;

use Robel\DataPipeline\Results\RecordErrorBag;
use RuntimeException;

class PipelineValidationException extends RuntimeException
{
    public function __construct(
        string $message,
        private readonly RecordErrorBag $errors,
    ) {
        parent::__construct($message);
    }

    public function errors(): RecordErrorBag
    {
        return $this->errors;
    }
}
