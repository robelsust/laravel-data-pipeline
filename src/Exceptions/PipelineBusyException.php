<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Exceptions;

use RuntimeException;

/** Another worker owns an uncommitted record or chunk; redelivery must wait. */
final class PipelineBusyException extends RuntimeException {}
