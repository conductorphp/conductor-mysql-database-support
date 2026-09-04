<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Database\DatabaseImportExportAdapterInterface;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Exception;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class ExportPlugin
{
    private const OPTION_IGNORE_TABLES = 'ignore_tables';
    private const OPTION_REMOVE_DEFINERS = 'remove_definers';

    private string $username;
    private string $password;
    private string $host;
    private int $port;
    private ShellAdapterInterface $shellAdapter;
    private LoggerInterface $logger;


    public function __construct(
        ShellAdapterInterface $shellAdapter,
        string                $username,
        string                $password,
        string                $host = 'localhost',
        int                   $port = 3306,
        ?LoggerInterface      $logger = null
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
    }

    public function exportToFile(
        string $database,
        string $path,
        array  $options = []
    ): string {
        $workingDir = $this->prepareWorkingDirectory($path);
        $path = realpath($path);
        $this->logger->info("Exporting database $database to file $path/$database.tgz");

        $this->assertIsUsable();
        $this->validateOptions($options);

        $command = $this->getMyDumperExportCommand($database, $options);
        $archive = "$workingDir/$database.tgz";

        try {
            // Every path in the command is relative, so the working directory has to be the command's
            // own. Left to the process's cwd, the dump and the archive land wherever conductor was
            // started from while the returned path claims they are under $path.
            $this->shellAdapter->runShellCommand($command, $workingDir, null, ShellAdapterInterface::PRIORITY_LOW);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }

        $this->assertArchiveIsRestorable($archive, $workingDir);

        $filename = "$path/$database.tgz";
        if (!rename($archive, $filename)) {
            throw new Exception\RuntimeException(
                sprintf('Failed to move the export archive from "%s" to "%s".', $archive, $filename)
            );
        }

        // Only on success. A failed export's working directory is the only evidence of what mydumper
        // did or did not write.
        $this->shellAdapter->runShellCommand('rm -rf ' . escapeshellarg($workingDir));

        return $filename;
    }

    /**
     * mydumper exiting 0 is not proof that it wrote anything: the shell adapter only sees the exit
     * status, so a command that produced nothing reads as a successful export. The caller is then
     * handed a path to a file that may not exist, and the snapshot is discovered to be bad at restore
     * time — months later, by someone who needed it.
     *
     * @throws Exception\RuntimeException If the archive is missing, empty, or not a mydumper dump
     */
    private function assertArchiveIsRestorable(string $archive, string $workingDir): void
    {
        clearstatcache(true, $archive);

        if (!is_file($archive)) {
            throw new Exception\RuntimeException(
                sprintf('mydumper reported success but wrote no archive at "%s".', $archive)
                . $this->describeLeftoverWorkingDir($workingDir)
            );
        }

        if (!filesize($archive)) {
            throw new Exception\RuntimeException(
                sprintf('mydumper reported success but wrote an empty archive at "%s".', $archive)
                . $this->describeLeftoverWorkingDir($workingDir)
            );
        }

        // Listing the archive reads all of it, which is the point: it is the only thing that proves the
        // gzip stream runs to the end. An export truncated by a full disk tars and exits 0, and would
        // otherwise be found out years later by whoever needed the snapshot.
        try {
            $contents = $this->shellAdapter->runShellCommand('tar -tzf ' . escapeshellarg($archive));
        } catch (\Exception $e) {
            throw new Exception\RuntimeException(
                sprintf('The export archive "%s" could not be read back: %s', $archive, $e->getMessage())
                . $this->describeLeftoverWorkingDir($workingDir)
            );
        }

        // myloader refuses a directory with no metadata file, so an archive without one is not a
        // restorable snapshot however well formed the tar is.
        if (!preg_match('~(^|/)metadata$~m', $contents)) {
            throw new Exception\RuntimeException(
                sprintf(
                    'The export archive "%s" contains no metadata file, so myloader cannot restore it.',
                    $archive
                )
                . $this->describeLeftoverWorkingDir($workingDir)
            );
        }
    }

    private function describeLeftoverWorkingDir(string $workingDir): string
    {
        return " The working directory \"$workingDir\" was left in place so the failure can be diagnosed.";
    }

    private function getMyDumperExportCommand(string $database, array $options): string
    {
        # Every path below is relative to the working directory this command is run in, which is the
        # directory the export owns. That also keeps find off the caller's cwd, which it cannot always
        # read and fails on when it cannot.
        # @link https://unix.stackexchange.com/questions/349894/can-i-tell-find-to-to-not-restore-initial-working-directory

        $dumpStructureCommand = 'mydumper --database ' . escapeshellarg($database) . ' --outputdir '
            . escapeshellarg($database) . ' --clear -v 3 --no-data --triggers --events --routines '
            . '--sync-thread-lock-mode=LOCK_ALL '
            . $this->getMysqldumperCommandConnectionArguments() . ' ';

        // 🚨 Read the option truthfully. This was `empty($options[...])`, which inverted it: the
        // rewrite ran only when the option was FALSY, so `remove_definers => true` DISABLED removal.
        // Not academic — ConductorCore\Console\Database\DatabaseExportCommand passes
        // `'remove_definers' => !$input->getOption('no-remove-definers')`, so with the flag absent it
        // sends `true`, and `empty(true)` is false. `conductor database:export` therefore stripped no
        // definers by default on ANY adapter, and `--no-remove-definers` was the only way to get
        // stripping. (CTAP-1607)
        //
        // Absent defaults to TRUE so a direct API caller that omits the option keeps the
        // strip-by-default behavior it has today; only an explicit false now keeps definers.
        //
        // ⚠️ Not `!empty(...)`. For any value that is PRESENT the two are identical — both reduce
        // to a truthy check — so the only thing that differs is the ABSENT case, and `!empty()`
        // reads absent as "keep". That would silently flip the direct API callers who omit the
        // option and have always had definers stripped, turning a polarity fix into a regression
        // for the one caller class the bug never touched. `?? true` states the default where it is
        // read, and it can tell "absent" from "explicitly false" — which `empty()` structurally
        // cannot, and which is how the original inversion hid for so long.
        if ($options[self::OPTION_REMOVE_DEFINERS] ?? true) {
            // Replace definer in triggers and views with CURRENT_USER
            $dumpStructureCommand .= '&& find ' . escapeshellarg($database)
                . ' \( -name "*-schema-view.sql" -o -name "*-schema-triggers.sql" \)'
                . ' -exec sed -ri \'s|DEFINER=[^ ]+ |DEFINER=CURRENT_USER |g\' {} \;';
        }

        $dumpDataCommand = 'mydumper --database ' . escapeshellarg($database) . ' --outputdir '
            . escapeshellarg($database) . ' --merge -v 3 --no-schemas '
            . '--sync-thread-lock-mode=LOCK_ALL '
            . $this->getMysqldumperCommandConnectionArguments() . ' ';

        $dataTables = $this->getDataTables($database, $options);
        if ($dataTables) {
            $dumpDataCommand .= '--tables-list ' . implode(',', $dataTables);
        }

        // @todo Move this to somewhere else. This is specific to a known Magento issue
        // Avoid issue with tables that defaults timestamp fields to '0000-00-00 00:00:00', which cause on error on
        // import
        $fixTimestampDefaultIssueCommand = 'find ' . escapeshellarg($database)
            . ' -name "*.sql" -exec sed -ri "s|(timestamp\|datetime) (NOT )?NULL DEFAULT '
            . '\'0000-00-00 00:00:00\'|\1 \2NULL DEFAULT CURRENT_TIMESTAMP|gI" {} \;';

        $tarCommand = 'tar -czf ' . escapeshellarg("$database.tgz") . ' ' . escapeshellarg($database);

        return "$dumpStructureCommand && $dumpDataCommand && $fixTimestampDefaultIssueCommand && $tarCommand";
    }

    private function getMysqldumperCommandConnectionArguments(): string
    {
        return sprintf(
            '-h %s -P %s -u %s %s',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            $this->password ? '-p ' . escapeshellarg($this->password) . ' ' : ''
        );
    }

    private function getDataTables(string $database, array $options): array
    {
        $dataTables = [];
        if (!empty($options[self::OPTION_IGNORE_TABLES])) {
            $command = 'mysql --skip-column-names --silent -e "SHOW TABLES from \`' . $database . '\`;" '
                . $this->getMysqlCommandConnectionArguments() . ' ';
            $tables = trim($this->shellAdapter->runShellCommand($command));
            if (!$tables) {
                return [];
            }

            $allTables = explode("\n", $tables);
            $ignoredTables = [];
            foreach ($options[self::OPTION_IGNORE_TABLES] as $pattern) {
                $ignoredTables += array_filter($allTables, function ($table) use ($pattern) {
                    return fnmatch($pattern, $table);
                });
            }
            $dataTables = array_diff($allTables, $ignoredTables);

            $mappedTables = [];
            foreach ($dataTables as $table) {
                $mappedTables[] = $database . '.' . $table;
            }
            $dataTables = $mappedTables;
        }

        return $dataTables;
    }

    /**
     * @return string Working directory
     * @throws Exception\RuntimeException If path is not writable
     */
    private function prepareWorkingDirectory(string $path): string
    {
        if (!(is_dir($path) && is_writable($path))) {
            throw new Exception\RuntimeException(
                sprintf(
                    'Path "%s" is not a writable directory.',
                    $path
                )
            );
        }

        $workingDir = realpath($path) . '/' . DatabaseImportExportAdapterInterface::DEFAULT_WORKING_DIR;
        if (!mkdir($workingDir) && !is_dir($workingDir)) {
            throw new Exception\RuntimeException(sprintf('Directory "%s" was not created', $workingDir));
        }
        return $workingDir;
    }

    /**
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        $validOptionKeys = [self::OPTION_IGNORE_TABLES, self::OPTION_REMOVE_DEFINERS];
        $invalidOptionKeys = array_diff(array_keys($options), $validOptionKeys);
        if ($invalidOptionKeys) {
            throw new Exception\DomainException('Invalid options ' . implode(', ', $invalidOptionKeys) . ' provided.');
        }
    }

    private function getMysqlCommandConnectionArguments(): string
    {
        return sprintf(
            '-h %s -P %s -u %s %s',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            $this->password ? '-p' . escapeshellarg($this->password) . ' ' : ''
        );
    }

    public function assertIsUsable(): void
    {
        try {
            if (!is_callable('exec')) {
                throw new RuntimeException('the "exec" function is not callable.');
            }

            $requiredFunctions = [
                'mysql',
                'mydumper',
                // The export tars its own output and reads the archive back to check it.
                'tar',
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

            // Every flag this plugin passes still exists in 1.0, so an older mydumper would export
            // fine. The floor is asserted here anyway: mydumper and myloader ship together, so an
            // image that would fail this check is one whose restores are already broken. Better to
            // say so when the snapshot is taken than when someone tries to restore it.
            MydumperVersion::assertSupported('mydumper');
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
        $this->logger = $logger;
    }
}
