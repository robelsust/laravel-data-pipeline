<?php

declare(strict_types=1);

namespace Robel\DataPipeline;

/** Shared storage values for run and checkpoint states. */
enum PipelineStatus: string
{
    case Created = 'CREATED';
    case Running = 'RUNNING';
    case Pending = 'PENDING';
    case Processing = 'PROCESSING';
    case Completed = 'COMPLETED';
    case Partial = 'PARTIAL';
    case PartialSuccess = 'PARTIAL_SUCCESS';
    case Failed = 'FAILED';
    case Paused = 'PAUSED';
}
