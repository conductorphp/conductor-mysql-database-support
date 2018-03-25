<?php

namespace ConductorMySqlSupport\Adapter\TabDelimited;

use ConductorCore\Database\DatabaseImportExportAdapterInterface;
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
        string $username,
        string $password,
        string $host = 'localhost',
        int $port = 3306,
        ShellAdapterInterface $shellAdapter = null,
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
                'mysql',
                'mysqldump',
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
        $workingDir = $this->prepareWorkingDirectory($path);
        $path = realpath($path);
        $this->logger->info("Exporting database $database to file $path/$database.tgz");
        $this->assertIsUsable();
        $this->validateOptions($options);

        $command = $this->getTabDelimitedFileExportCommand(
            $database,
            $workingDir,
            $options
        );

        try {
            $this->shellAdapter->runShellCommand($command, null, null, ShellAdapterInterface::PRIORITY_LOW);
            $this->shellAdapter->runShellCommand('rm -rf ' . escapeshellarg($workingDir));

        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }

        return "$path/$database.tgz";
    }

    /**
     * @param       $database
     * @param       $workingDir
     * @param array $options
     *
     * @return string
     */
    private function getTabDelimitedFileExportCommand(
        string $database,
        string $workingDir,
        array $options
    ): string {
        $dumpStructureCommand = $this->getDumpStructureCommand($database, $options)
            . '> ' . escapeshellarg("$workingDir/schema.sql");

        $dataTables = $this->getDataTables($database, $options);
        $dumpDataCommand = '';
        if ($dataTables) {
            $rowsPerFile = 100000;
            foreach ($dataTables as $table) {
                $numRowsCommand = 'mysql ' . escapeshellarg($database)
                    . ' --skip-column-names --silent -e "SELECT COUNT(*) FROM \`' . $table . '\`" '
                    . $this->getMysqlCommandConnectionArguments() . ' ';
                $numRows = (int)$this->shellAdapter->runShellCommand($numRowsCommand);
                if (0 == $numRows) {
                    continue;
                }

                $getPrimaryKeyCommand = 'mysql ' . escapeshellarg($database) . ' --skip-column-names --silent -e '
                    . '"SELECT \`COLUMN_NAME\` FROM \`information_schema\`.\`COLUMNS\` '
                    . 'WHERE \`TABLE_SCHEMA\` = ' . escapeshellarg($database) . ' '
                    . 'AND \`TABLE_NAME\` = ' . escapeshellarg($table) . ' '
                    . 'AND \`COLUMN_KEY\` = \'PRI\'" '
                    . $this->getMysqlCommandConnectionArguments() . ' ';
                $primaryKeys = trim($this->shellAdapter->runShellCommand($getPrimaryKeyCommand));
                if ($primaryKeys) {
                    $orderBy = 'ORDER BY \`' . implode('\`,\`', explode("\n", $primaryKeys)) . '\` ';
                } else {
                    $orderBy = '';
                }

                $numFiles = ceil($numRows / $rowsPerFile);
                $fileNumber = 1;
                for ($i = 0; $i < $numRows; $i += $rowsPerFile) {
                    $dumpDataCommand .= "echo 'Exporting \"$table\" data [$fileNumber/$numFiles].' 1>&2 && "
                        . 'mysql ' . escapeshellarg($database) . ' --skip-column-names -e "SELECT * FROM \`' . $table
                        . '\` '
                        . $orderBy
                        . 'LIMIT ' . $i . ',' . ($i + $rowsPerFile) . '" '
                        . '> ' . escapeshellarg("$workingDir/$table.$fileNumber.txt") . ' '
                        . '&& ';
                    $fileNumber++;
                }
            }
            $dumpDataCommand = substr($dumpDataCommand, 0, -4);
        }
        $tarCommand = 'cd ' . escapeshellarg(dirname($workingDir)) . ' && '
            . 'tar czf ' . escapeshellarg("$database.tgz") . ' ' . escapeshellarg(basename($workingDir));

        $command = $dumpStructureCommand;
        if ($dumpDataCommand) {
            $command .= " && $dumpDataCommand";
        }
        $command .= "&& $tarCommand";
        return $command;
    }

    /**
     * @return string
     */
    private function getMysqlCommandConnectionArguments(): string
    {
        return sprintf(
            '-h %s -P %s -u %s -p%s ',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            escapeshellarg($this->password)
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
            . $this->getMysqlCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false --skip-comments --no-data --verbose ';

        if (!empty($options[self::OPTION_REMOVE_DEFINERS])) {
            $dumpStructureCommand .= '| sed "s/DEFINER=[^*]*\*/\*/g" ';
        }

        return $dumpStructureCommand;
    }

    /**
     * @param string $path
     *
     * @throws Exception\RuntimeException If path is not writable
     * @return string Working directory
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
        if (!is_dir($workingDir)) {
            mkdir($workingDir);
        }
        return $workingDir;
    }

    /**
     * @param array $options
     *
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        $validOptionKeys = [self::OPTION_REMOVE_DEFINERS, self::OPTION_IGNORE_TABLES];
        $invalidOptionKeys = array_diff(array_keys($options), $validOptionKeys);
        if ($invalidOptionKeys) {
            throw new Exception\DomainException('Invalid options ' . implode(', ', $invalidOptionKeys) . ' provided.');
        }
    }

    /**
     * @param string $database
     * @param array  $options
     *
     * @return array
     */
    private function getDataTables(string $database, array $options): array
    {
        $command = 'mysql --skip-column-names --silent -e "SHOW TABLES from \`' . $database . '\`;" '
            . $this->getMysqlCommandConnectionArguments() . ' ';
        $result = trim($this->shellAdapter->runShellCommand($command));
        if (!$result) {
            return [];
        }

        $dataTables = explode("\n", $result);
        if (!empty($options[self::OPTION_IGNORE_TABLES])) {
            $dataTables = array_diff($dataTables, $options[self::OPTION_IGNORE_TABLES]);
        }
        return $dataTables;
    }

    /**
     * @inheritdoc
     */
    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }
}
