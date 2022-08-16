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

        try {
            $this->shellAdapter->runShellCommand($command, null, null, ShellAdapterInterface::PRIORITY_LOW);
            $this->shellAdapter->runShellCommand('pwd && rm -rf ' . escapeshellarg($database), $workingDir);

        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }

        return "$path/$database.tgz";
    }

    private function getMyDumperExportCommand(string $database, array $options): string
    {
        # find command has a bug where it will fail if you do not have read permissions to the current working directory
        # Temporarily switching into the working directory while running this command.
        # @link https://unix.stackexchange.com/questions/349894/can-i-tell-find-to-to-not-restore-initial-working-directory

        $dumpStructureCommand = 'mydumper --database ' . escapeshellarg($database) . ' --outputdir '
            . escapeshellarg($database) . ' -v 3 --no-data --triggers --events --routines --lock-all-tables '
            . $this->getMysqldumperCommandConnectionArguments() . ' ';

        if (empty($options[self::OPTION_REMOVE_DEFINERS])) {
            // Replace definer in triggers and views with CURRENT_USER
            $dumpStructureCommand .= '&& find ' . escapeshellarg($database)
                . ' \( -name "*-schema-view.sql" -o -name "*-schema-triggers.sql" \)'
                . ' -exec sed -ri \'s|DEFINER=[^ ]+ |DEFINER=CURRENT_USER |g\' {} \;';
        }

        $dumpDataCommand = 'mydumper --database ' . escapeshellarg($database) . ' --outputdir '
            . escapeshellarg($database) . ' -v 3 --no-schemas --lock-all-tables '
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
            $allTables = explode("\n", trim($this->shellAdapter->runShellCommand($command)));
            $ignoredTables = [];
            foreach ($options[self::OPTION_IGNORE_TABLES] as $pattern) {
                $ignoredTables += array_filter($allTables, function ($table) use ($pattern) {
                    return fnmatch($pattern, $table);
                });
            }
            $dataTables = array_diff($allTables, $ignoredTables);
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

    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }
}
