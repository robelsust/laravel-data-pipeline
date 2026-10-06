<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Sources;

use InvalidArgumentException;
use Robel\DataPipeline\Contracts\SourceInterface;
use RuntimeException;

final readonly class CsvSource implements SourceInterface
{
    /**
     * @param  string  $filePath  Absolute or relative path to CSV file
     * @param  bool  $hasHeaders  Whether first line contains column headers
     * @param  string  $delimiter  CSV delimiter (default ',')
     * @param  string  $enclosure  CSV enclosure (default '"')
     * @param  string  $escape  CSV escape char (default '\\')
     */
    public function __construct(
        private string $filePath,
        private bool $hasHeaders = true,
        private string $delimiter = ',',
        private string $enclosure = '"',
        private string $escape = '\\',
    ) {
        if (! file_exists($filePath) || ! is_readable($filePath)) {
            throw new InvalidArgumentException("CSV file [{$filePath}] does not exist or is not readable.");
        }
    }

    /** @return \Generator<int, array<int|string, mixed>> */
    public function records(): iterable
    {
        $handle = fopen($this->filePath, 'r');
        if ($handle === false) {
            throw new RuntimeException("Failed to open CSV file [{$this->filePath}].");
        }

        try {
            $headers = [];
            $rowNumber = 0;

            if ($this->hasHeaders) {
                $rawHeaders = fgetcsv($handle, 0, $this->delimiter, $this->enclosure, $this->escape);
                if ($rawHeaders !== false) {
                    $headers = array_map(fn ($h) => trim((string) $h), $rawHeaders);
                }
            }

            while (($row = fgetcsv($handle, 0, $this->delimiter, $this->enclosure, $this->escape)) !== false) {
                $rowNumber++;

                // Skip completely empty lines
                if (count($row) === 1 && $row[0] === null) {
                    continue;
                }

                if ($this->hasHeaders && ! empty($headers)) {
                    $record = [];
                    foreach ($headers as $colIdx => $colName) {
                        $record[$colName] = $row[$colIdx] ?? null;
                    }
                    yield $rowNumber => $record;
                } else {
                    yield $rowNumber => $row;
                }
            }
        } finally {
            fclose($handle);
        }
    }

    public function count(): ?int
    {
        return null; // Unknown without scanning file
    }

    public function fingerprint(): string
    {
        return 'csv:'.md5_file($this->filePath);
    }
}
