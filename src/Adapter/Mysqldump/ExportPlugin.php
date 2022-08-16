<?php

namespace ConductorMySqlSupport\Adapter\Mysqldump;

use ConductorCore\Exception;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ExportPlugin
{
    const OPTION_IGNORE_TABLES = 'ignore_tables';
    const OPTION_REMOVE_DEFINERS = 'remove_definers';

    /**
     * @var string
     */
    private $username;
    /**
     * @var string
     */
    private $password;
    /**
     * @var string
     */
    private $host;
    /**
     * @var int
     */
    private $port;
    /**
     * @var ShellAdapterInterface
     */
    private $shellAdapter;
    /**
     * @var null|LoggerInterface
     */
    private $logger;


    public function __construct(
        ShellAdapterInterface $shellAdapter,
        string $username,
        string $password,
        string $host = 'localhost',
        int $port = 3306,
        LoggerInterface $logger = null
    ) {
        if (is_null($logger)) {
            $logger = new NullLogger();
        }
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
        $this->shellAdapter = $shellAdapter;
        $this->logger = $logger;
    }

    /**
     * @inheritdoc
     */
    public function assertIsUsable(): void
    {
        try {
            if (!is_callable('exec')) {
                throw new \Exception('the "exec" function is not callable.');
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
                throw new \Exception(
                    sprintf(
                        'the "%s" shell function(s) are not available.',
                        implode('", "', $missingFunctions)
                    )
                );
            }
        } catch (\Exception $e) {
            throw new Exception\RuntimeException(
                sprintf(
                    __CLASS__
                    . ' is not usable in this environment because ' . $e->getMessage()
                )
            );
        }
    }

    /**
     * @inheritdoc
     */
    public function exportToFile(
        string $database,
        string $path,
        array $options = []
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
     * @param string $path
     *
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

    /**
     * @return string
     */
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

    /**
     * @param string $database
     * @param array  $options
     *
     * @return string
     */
    private function getDumpStructureCommand(string $database, array $options): string
    {
        $dumpStructureCommand = 'mysqldump ' . escapeshellarg($database) . ' '
            . $this->getCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false --skip-comments --no-data --no-autocommit --verbose ';

        if (empty($options[self::OPTION_REMOVE_DEFINERS])) {
            // Replace definer with CURRENT_USER
            $dumpStructureCommand .= '| sed "s|DEFINER=[^ ]+ |DEFINER=CURRENT_USER |g" ';
        }

        return $dumpStructureCommand;
    }

    /**
     * @param array $options
     *
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

    /**
     * @param LoggerInterface $logger
     *
     * @return void
     */
    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }

}
