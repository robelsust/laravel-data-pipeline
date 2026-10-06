<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Exceptions;

use Illuminate\Database\QueryException;
use Throwable;

/** Keeps SQL bindings out of persisted failure diagnostics. */
final class PersistenceError
{
    public static function describe(Throwable $exception): string
    {
        if ($exception instanceof QueryException) {
            return 'Database persistence failed (SQLSTATE '.($exception->errorInfo[0] ?? 'unknown').').';
        }

        return $exception->getMessage();
    }
}
