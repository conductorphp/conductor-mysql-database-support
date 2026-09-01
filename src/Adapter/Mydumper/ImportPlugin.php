<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Exception;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ImportPlugin
{
    /**
     * Appended to the extracted dump directory's path — a sibling, not a file inside it, so myloader
     * never sees it.
     */
    private const ERROR_LOG_SUFFIX = '.myloader.log';

    private const MAX_REPORTED_ERRORS = 10;

    /**
     * Matches myloader's own fatal lines and their timestamp prefix, e.g.
     * "** (myloader:26): CRITICAL **: 21:40:12.916: Thread 5 ... - ERROR 1101: ...".
     *
     * The level has to be exactly CRITICAL so that glib's own "GLib-CRITICAL" chatter, which myloader
     * emits on a healthy run too, does not get reported as the reason for the failure.
     */
    private const MYLOADER_CRITICAL_PATTERN = '/^\*\* \([^)]+\): CRITICAL \*\*: [0-9:.]+: /';

    private string $username;
    private string $password;
    private string $host;
    private int $port;
    private ShellAdapterInterface $shellAdapter;
    private LoggerInterface $logger;
    private SqlModeSanitizer $sqlModeSanitizer;
    private RestorePreflight $restorePreflight;


    public function __construct(
        ShellAdapterInterface $shellAdapter,
        string                $username,
        string                $password,
        string                $host = 'localhost',
        int                   $port = 3306,
        ?LoggerInterface      $logger = null,
        ?SqlModeSanitizer     $sqlModeSanitizer = null,
        ?RestorePreflight     $restorePreflight = null
    ) {
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
        $this->shellAdapter = $shellAdapter;
        if (is_null($logger)) {
            $logger = new NullLogger();
        }
        $this->logger = $logger;
        if (is_null($sqlModeSanitizer)) {
            $sqlModeSanitizer = new SqlModeSanitizer(
                new TargetSqlModeSupport($username, $password, $host, $port, $logger),
                $logger
            );
        }
        $this->sqlModeSanitizer = $sqlModeSanitizer;
        if (is_null($restorePreflight)) {
            $restorePreflight = new RestorePreflight(
                new TargetSchemaSupport($username, $password, $host, $port, $logger),
                $logger
            );
        }
        $this->restorePreflight = $restorePreflight;
    }

    public function importFromFile(
        string $filename,
        string $database,
        array  $options = []
    ): void {
        $this->logger->info("Importing file $filename into database $database");
        $this->assertIsUsable();
        $this->validateOptions($options);
        $extractedPath = $this->extractAndValidateImportFile($filename);
        $this->sqlModeSanitizer->sanitize($extractedPath);

        $errorLog = $extractedPath . self::ERROR_LOG_SUFFIX;
        $command = $this->getMyDumperImportCommand($database, $extractedPath, $errorLog);

        try {
            $this->restorePreflight->check($extractedPath, $database);
            $this->shellAdapter->runShellCommand($command, null, null, ShellAdapterInterface::PRIORITY_LOW);
        } catch (\Exception $e) {
            $this->logger->error($this->describeLeftoverEvidence($extractedPath, $errorLog));
            throw new Exception\RuntimeException($this->describeImportFailure($e, $errorLog), 0, $e);
        }

        // Only on success. A failed restore's dump and log are the only evidence of what went wrong.
        $this->shellAdapter->runShellCommand(
            'rm -rf ' . escapeshellarg($extractedPath) . ' ' . escapeshellarg($errorLog)
        );
    }

    /**
     * A failed restore is the one time the extracted dump is worth keeping, so say where it is rather
     * than leaving it as an unexplained directory. The error log only exists if myloader itself ran —
     * a restore refused before that point never created one.
     */
    private function describeLeftoverEvidence(string $extractedPath, string $errorLog): string
    {
        $message = "The extracted dump was left at \"$extractedPath\"";
        if (is_file($errorLog)) {
            $message .= " and myloader's output at \"$errorLog\"";
        }

        return $message . ' so the failure can be diagnosed. Safe to delete.';
    }

    /**
     * myloader aborts the process on a failed statement — with a core dump, which is all the shell
     * adapter's exception can report. The reason is on myloader's stderr, so pull it back out and put
     * it in the message. A schema the target rejects (a JSON column with a DEFAULT, an over-long
     * index) has to name itself, or the failure looks like a crash rather than an incompatible dump.
     */
    private function describeImportFailure(\Exception $exception, string $errorLog): string
    {
        $reasons = $this->getMyLoaderErrors($errorLog);
        if (!$reasons) {
            return $exception->getMessage();
        }

        return "myloader failed to restore the dump:\n  " . implode("\n  ", $reasons)
            . "\n" . $exception->getMessage();
    }

    /**
     * @return string[] Distinct fatal errors reported by myloader, in the order it hit them
     */
    private function getMyLoaderErrors(string $errorLog): array
    {
        if (!is_readable($errorLog)) {
            return [];
        }

        $log = file($errorLog, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        if (false === $log) {
            return [];
        }

        $reasons = [];
        foreach ($log as $line) {
            if (!preg_match(self::MYLOADER_CRITICAL_PATTERN, $line)) {
                continue;
            }
            $reason = trim(preg_replace(self::MYLOADER_CRITICAL_PATTERN, '', $line));
            if ('' !== $reason) {
                $reasons[$reason] = $reason;
            }
        }

        return array_slice(array_values($reasons), 0, self::MAX_REPORTED_ERRORS);
    }

    /**
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        if ($options) {
            throw new Exception\DomainException(__CLASS__ . ' does not currently support any options.');
        }
    }


    private function getMyDumperImportCommand(string $database, string $importDir, string $errorLog): string
    {
        // --drop-table=DROP is mydumper 1.0's spelling of what 0.x called --overwrite-tables. Passing
        // the mode explicitly rather than relying on the bare flag's default keeps it readable, and
        // avoids the option's optional argument swallowing whatever token follows it.
        //
        // tee keeps myloader's progress streaming to the shell adapter while also capturing it. The
        // adapter reads stderr to EOF, which only happens once tee has exited, so the log file is
        // complete by the time the command returns.
        return 'myloader --database ' . escapeshellarg($database) . ' --directory '
            . escapeshellarg($importDir) . ' -v 3 --drop-table=DROP '
            . $this->getMysqlCommandConnectionArguments()
            . ' 2> >(tee ' . escapeshellarg($errorLog) . ' >&2)';
    }

    private function getMysqlCommandConnectionArguments(): string
    {
        return sprintf(
            '-h %s -P %s -u %s %s',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            $this->password ? '-p ' . escapeshellarg($this->password) . ' ' : ''
        );
    }


    /**
     * @return string Extracted directory path
     * @throws Exception\RuntimeException If file extension or format invalid
     */
    private function extractAndValidateImportFile(string $filename): string
    {
        if (0 !== strcasecmp('.tgz', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .tgz.');
        }

        $path = realpath(dirname($filename));
        $this->shellAdapter->runShellCommand(
            'cd ' . escapeshellarg($path)
            . ' && tar xzf ' . escapeshellarg(basename($filename))
            . ' && rm -f ' . escapeshellarg(basename($filename))
        );

        $files = array_slice(scandir($path), 2);
        if (1 !== count($files)) {
            throw new Exception\RuntimeException("File \"$filename\" is not a valid mydumper export.");
        }

        $extractedPath = "$path/{$files[0]}";

        // Fix metadata file if it was created by an old version of mydumper
        $this->fixMetadataFile($extractedPath);

        return $extractedPath;
    }

    /**
     * Fix metadata file from old mydumper versions that don't include the [snapshot] group header
     * and use old text format instead of key=value format
     */
    private function fixMetadataFile(string $extractedPath): void
    {
        $metadataFile = $extractedPath . '/metadata';

        if (!file_exists($metadataFile)) {
            $this->logger->warning("Metadata file not found at $metadataFile");
            return;
        }

        $content = file_get_contents($metadataFile);
        if ($content === false) {
            $this->logger->warning("Failed to read metadata file at $metadataFile");
            return;
        }

        // A group header anywhere means this is already the key=value format. It is not necessarily the
        // FIRST line: mydumper 0.19 opens the file with a "# Started dump at: ..." comment and only then
        // writes [config], [myloader_session_variables] and a group per table. Testing only the first
        // line sent those dumps through the conversion below, which drops every line that is neither a
        // comment nor a key=value pair — including all of those group headers.
        if (preg_match('/^\s*\[.+\]\s*$/m', $content)) {
            return;
        }

        // Old mydumper format has lines like:
        // "Started dump at: 2025-11-12 09:00:08"
        // "Finished dump at: 2025-11-12 09:00:08"
        // New format needs:
        // [snapshot]
        // started=2025-11-12 09:00:08
        // finished=2025-11-12 09:00:08

        $lines = explode("\n", $content);
        $fixedLines = ['[snapshot]'];
        $converted = false;

        foreach ($lines as $line) {
            $trimmedLine = trim($line);

            // Skip empty lines
            if ($trimmedLine === '') {
                continue;
            }

            // Convert "Started dump at: TIMESTAMP" to "started=TIMESTAMP"
            if (preg_match('/^Started dump at:\s*(.+)$/i', $trimmedLine, $matches)) {
                $fixedLines[] = 'started=' . trim($matches[1]);
                $converted = true;
                continue;
            }

            // Convert "Finished dump at: TIMESTAMP" to "finished=TIMESTAMP"
            if (preg_match('/^Finished dump at:\s*(.+)$/i', $trimmedLine, $matches)) {
                $fixedLines[] = 'finished=' . trim($matches[1]);
                $converted = true;
                continue;
            }

            // If it's already a key=value line, keep it as-is
            if (strpos($trimmedLine, '=') !== false) {
                $fixedLines[] = $line;
                $converted = true;
                continue;
            }

            // Skip any other descriptive text or comments
            if (strpos($trimmedLine, '#') === 0) {
                $fixedLines[] = $line;
            }
        }

        if (!$converted) {
            // No valid data found, just add the header
            $this->logger->warning("No valid metadata found in file, adding minimal [snapshot] header");
        }

        $fixedContent = implode("\n", $fixedLines) . "\n";

        if (file_put_contents($metadataFile, $fixedContent) === false) {
            $this->logger->warning("Failed to fix metadata file at $metadataFile");
            return;
        }

        $this->logger->info("Fixed metadata file from old mydumper version by converting to [snapshot] format");
    }

    public function assertIsUsable(): void
    {
        try {
            if (!is_callable('exec')) {
                throw new Exception\RuntimeException('the "exec" function is not callable.');
            }

            $requiredFunctions = [
                'tar',
                'myloader',
            ];
            $missingFunctions = [];
            foreach ($requiredFunctions as $requiredFunction) {
                exec('which ' . escapeshellarg($requiredFunction) . ' &> /dev/null', $output, $return);
                if (0 != $return) {
                    $missingFunctions[] = $requiredFunction;
                }
            }

            if ($missingFunctions) {
                throw new Exception\RuntimeException(
                    sprintf(
                        'the "%s" shell function(s) are not available.',
                        implode('", "', $missingFunctions)
                    )
                );
            }

            MydumperVersion::assertSupported('myloader');
        } catch (\Exception $e) {
            throw new Exception\RuntimeException(
                sprintf(
                    '%s is not usable in this environment because %s.',
                    __CLASS__,
                    $e->getMessage()
                )
            );
        }
    }

    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->sqlModeSanitizer->setLogger($logger);
        $this->restorePreflight->setLogger($logger);
        $this->logger = $logger;
    }
}
