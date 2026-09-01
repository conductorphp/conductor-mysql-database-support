<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Removes sql_mode values the target server does not recognize from an extracted mydumper dump.
 *
 * mydumper records the SOURCE server's sql_mode and myloader replays it, both per connection (the
 * metadata file's [myloader_session_variables] group) and per file (a `SET SQL_MODE=...` header in
 * every .sql). A MariaDB source contributes NO_AUTO_CREATE_USER, which MySQL 8 removed, so every
 * myloader thread dies on connection setup with ERROR 1231 before a single row is read.
 *
 * The `/*!40101 *\/` gate the header is wrapped in does not help: version-gated comments run on
 * servers at or ABOVE that version, so MySQL 8 executes the statement and rejects the mode. Gates
 * protect against older servers, never newer ones.
 *
 * Scope is deliberately sql_mode and nothing else. It is a session setting the target no longer
 * knows, so dropping it changes nothing about the data. Schema DDL in the dump is left exactly as
 * dumped — a tool that quietly edits a production schema on the way in is a worse problem than the
 * one it solves.
 */
class SqlModeSanitizer
{
    /**
     * How much of each .sql file to inspect. mydumper writes its SET header as the first few lines;
     * the rest of the file is CREATE TABLE / INSERT and is never touched.
     */
    private const HEADER_BYTES = 8192;

    /**
     * Matches both forms mydumper writes, anchored to the start of a line:
     *
     *   /*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,...'*\/;   (every .sql file)
     *   SQL_MODE='NO_AUTO_VALUE_ON_ZERO,...' /*!40101           (the metadata file)
     *
     * Group 1 is everything up to and including the opening quote, group 2 the mode list.
     */
    private const SQL_MODE_PATTERN
        = '~^((?:/\*![0-9]+\s+)?(?:SET\s+(?:SESSION\s+|GLOBAL\s+)?)?SQL_MODE\s*=\s*\')([^\']*)\'~im';

    private LoggerInterface $logger;
    private TargetSqlModeSupport $targetSqlModeSupport;

    public function __construct(
        TargetSqlModeSupport $targetSqlModeSupport,
        ?LoggerInterface     $logger = null
    ) {
        $this->targetSqlModeSupport = $targetSqlModeSupport;
        $this->logger = $logger ?? new NullLogger();
    }

    /**
     * @param string $extractedPath Path to an extracted mydumper dump directory
     */
    public function sanitize(string $extractedPath): void
    {
        $files = $this->getDumpFiles($extractedPath);
        if (!$files) {
            return;
        }

        $modes = $this->findSqlModes($files);
        if (!$modes) {
            return;
        }

        $unsupportedModes = $this->targetSqlModeSupport->getUnsupportedModes($modes);
        if (!$unsupportedModes) {
            return;
        }

        $modifiedFiles = 0;
        foreach ($files as $file) {
            if ($this->removeModesFromFile($file, $unsupportedModes)) {
                $modifiedFiles++;
            }
        }

        $this->logger->notice(
            sprintf(
                'Removed sql_mode value(s) "%s" from %d file(s) of the dump. The source server set these and the '
                . 'target server does not accept them. sql_mode is a session setting; no data or schema was changed.',
                implode('", "', $unsupportedModes),
                $modifiedFiles
            )
        );
    }

    /**
     * @return string[]
     */
    private function getDumpFiles(string $extractedPath): array
    {
        $files = glob($extractedPath . '/*.sql') ?: [];

        $metadataFile = $extractedPath . '/metadata';
        if (is_file($metadataFile)) {
            array_unshift($files, $metadataFile);
        }

        return $files;
    }

    /**
     * @param string[] $files
     * @return string[] Every distinct sql_mode value named anywhere in the dump
     */
    private function findSqlModes(array $files): array
    {
        $modes = [];
        foreach ($files as $file) {
            $handle = @fopen($file, 'r');
            if (false === $handle) {
                $this->logger->warning("Could not read \"$file\" while checking the dump's sql_mode.");
                continue;
            }

            $chunk = $this->readInspectableChunk($handle, $file);
            fclose($handle);

            if (preg_match_all(self::SQL_MODE_PATTERN, $chunk, $matches)) {
                foreach ($matches[2] as $modeList) {
                    foreach ($this->splitModes($modeList) as $mode) {
                        $modes[$mode] = $mode;
                    }
                }
            }
        }

        return array_values($modes);
    }

    /**
     * @param string[] $unsupportedModes
     * @return bool Whether the file was modified
     */
    private function removeModesFromFile(string $file, array $unsupportedModes): bool
    {
        $handle = @fopen($file, 'r+');
        if (false === $handle) {
            $this->logger->warning("Could not open \"$file\" to remove unsupported sql_mode values from it.");
            return false;
        }

        $chunk = $this->readInspectableChunk($handle, $file);
        $sanitizedChunk = preg_replace_callback(
            self::SQL_MODE_PATTERN,
            function (array $matches) use ($unsupportedModes): string {
                $modes = $this->splitModes($matches[2]);
                $keptModes = array_diff($modes, $unsupportedModes);
                if (count($keptModes) === count($modes)) {
                    return $matches[0];
                }

                $keptModeList = implode(',', $keptModes);

                // Pad back to the original byte length so only this chunk has to be written. Dumps run
                // to gigabytes; rewriting every file end to end to shorten one header line would double
                // the I/O of a restore. Trailing spaces are insignificant in both forms mydumper writes.
                return $matches[1] . $keptModeList . "'"
                    . str_repeat(' ', strlen($matches[2]) - strlen($keptModeList));
            },
            $chunk
        );

        if (null === $sanitizedChunk || $sanitizedChunk === $chunk) {
            fclose($handle);
            return false;
        }

        rewind($handle);
        $written = fwrite($handle, $sanitizedChunk);
        fclose($handle);

        if (false === $written) {
            $this->logger->warning("Failed to write sanitized sql_mode header back to \"$file\".");
            return false;
        }

        return true;
    }

    /**
     * The metadata file is small and myloader reads all of it, so it is inspected whole. A .sql file
     * is read only as far as its header, truncated at the last complete line so a partial line can
     * never be rewritten.
     *
     * @param resource $handle
     */
    private function readInspectableChunk($handle, string $file): string
    {
        if ('metadata' === basename($file)) {
            return (string)stream_get_contents($handle);
        }

        $chunk = (string)fread($handle, self::HEADER_BYTES);
        if (strlen($chunk) < self::HEADER_BYTES) {
            return $chunk;
        }

        $lastNewline = strrpos($chunk, "\n");
        return false === $lastNewline ? '' : substr($chunk, 0, $lastNewline + 1);
    }

    /**
     * @return string[]
     */
    private function splitModes(string $modeList): array
    {
        return array_values(array_filter(array_map('trim', explode(',', $modeList)), 'strlen'));
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
        $this->targetSqlModeSupport->setLogger($logger);
    }
}
