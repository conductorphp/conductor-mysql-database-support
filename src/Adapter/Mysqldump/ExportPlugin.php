<?php

namespace ConductorMySqlSupport\Adapter\Mysqldump;

use ConductorCore\Exception;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

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

    public function assertIsUsable(): void
    {
        try {
            if (!is_callable('exec')) {
                throw new Exception\RuntimeException('the "exec" function is not callable.');
            }

            $requiredFunctions = [
                'gzip',
                'mysqldump',
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

    public function exportToFile(
        string $database,
        string $path,
        array  $options = []
    ): string {
        $this->prepareWorkingDirectory($path);
        $path = realpath($path);
        $this->logger->info("Exporting database $database to file $path/$database.sql.gz");

        $this->assertIsUsable();
        $this->validateOptions($options);

        $dumpStructureCommand = $this->getDumpStructureCommand($database, $options);
        $dumpDataCommand = 'mysqldump ' . escapeshellarg($database) . ' '
            . $this->getCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false --no-autocommit '
            . '--order-by-primary --skip-comments --no-create-db --no-create-info --skip-triggers ';
        if (!empty($options[self::OPTION_IGNORE_TABLES])) {
            $command = 'mysql --skip-column-names --silent -e "SHOW TABLES from \`' . $database . '\`;" '
                . $this->getCommandConnectionArguments() . ' ';
            $allTables = explode("\n", trim($this->shellAdapter->runShellCommand($command)));
            $ignoredTables = [];
            foreach ($options[self::OPTION_IGNORE_TABLES] as $pattern) {
                $ignoredTables += array_filter($allTables, function ($table) use ($pattern) {
                    return fnmatch($pattern, $table);
                });
            }
            foreach ($ignoredTables as $ignoredTable) {
                $dumpDataCommand .= '--ignore-table=' . escapeshellarg("$database.$ignoredTable") . ' ';
            }
        }

        $command = "($dumpStructureCommand && $dumpDataCommand) "
            . '| gzip -9 > ' . escapeshellarg("$path/$database.sql.gz");

        try {
            $this->shellAdapter->runShellCommand($command, null, null, ShellAdapterInterface::PRIORITY_LOW);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }

        return "$path/$database.sql.gz";
    }

    /**
     * @throws Exception\RuntimeException If path is not writable
     */
    private function prepareWorkingDirectory(string $path): void
    {
        if (!(is_dir($path) && is_writable($path))) {
            throw new Exception\RuntimeException(
                sprintf(
                    'Path "%s" is not a writable directory.',
                    $path
                )
            );
        }
    }

    private function getCommandConnectionArguments(): string
    {
        return sprintf(
            '-h %s -P %s -u %s %s',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            $this->password ? '-p' . escapeshellarg($this->password) . ' ' : ''
        );
    }

    private function getDumpStructureCommand(string $database, array $options): string
    {
        $dumpStructureCommand = 'mysqldump ' . escapeshellarg($database) . ' '
            . $this->getCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false --skip-comments --no-data --no-autocommit --verbose ';

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
            // Replace definer with CURRENT_USER
            // ⚠️ `-r` is load-bearing. Without it sed uses a BASIC regular expression, where `+` is a
            // literal plus character, so the pattern can never match a real definer and the rewrite
            // is a silent no-op. Verified against `CREATE TRIGGER DEFINER=`dev`@`%` foo`: unchanged
            // under BRE, rewritten under ERE. The Mydumper adapter always had `sed -ri`, which is
            // why this went unnoticed — that is the common path. (CTAP-1607)
            $dumpStructureCommand .= '| sed -r "s|DEFINER=[^ ]+ |DEFINER=CURRENT_USER |g" ';
        }

        return $dumpStructureCommand;
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

    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }

}
