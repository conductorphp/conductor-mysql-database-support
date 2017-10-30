<?php

namespace DevopsToolMySqlSupport\Adapter\TabDelimited;

use DevopsToolCore\Database\DatabaseExportAdapterInterface;
use DevopsToolCore\Exception;
use DevopsToolCore\ShellCommandHelper;
use DevopsToolMySqlSupport\Adapter\DatabaseConfig;
use Monolog\Handler\NullHandler;
use Psr\Log\LoggerInterface;

class MysqlTabDelimitedExportAdapter implements DatabaseExportAdapterInterface
{
    const OPTION_IGNORE_TABLES = 'ignore_tables';
    const OPTION_REMOVE_DEFINERS = 'remove_definers';

    /**
     * @var DatabaseConfig
     */
    private $databaseConfig;
    /**
     * @var ShellCommandHelper
     */
    private $shellCommandHelper;
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * MysqlTabDelimitedExportAdapter constructor.
     *
     * @param DatabaseConfig|null     $databaseConfig
     * @param ShellCommandHelper|null $shellCommandHelper
     * @param LoggerInterface|null    $logger
     */
    public function __construct(
        DatabaseConfig $databaseConfig = null,
        ShellCommandHelper $shellCommandHelper = null,
        LoggerInterface $logger = null
    ) {
        $this->databaseConfig = $databaseConfig;
        $this->shellCommandHelper = $shellCommandHelper;
        if (is_null($logger)) {
            $logger = new NullHandler();
        }
        $this->logger = $logger;
    }

    /**
     * @inheritdoc
     */
    public function assertIsUsable()
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
    public function getOptionsHelp()
    {
        return [
            self::OPTION_IGNORE_TABLES   => 'An array of table names to ignore data from when exporting.',
            self::OPTION_REMOVE_DEFINERS => 'A boolean flag for whether to remove definers for triggers. Useful if planning to '
                . 'import into a separate MySQL instance that does not have the users to match the definers.',
        ];
    }

    /**
     * @inheritdoc
     */
    public function exportToFile(
        $database,
        $path,
        array $options = []
    ) {
        $this->assertIsUsable();
        $this->validateOptions($options);
        $workingDir = $this->prepareWorkingDirectory($path);

        $command = $this->getTabDelimitedFileExportCommand(
            $database,
            $workingDir,
            $options
        );

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
            $this->shellCommandHelper->runShellCommand('rm -rf ' . escapeshellarg($workingDir));

        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }

        return realpath($path) . "/$database.tgz";
    }

    /**
     * @inheritdoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @param       $database
     * @param       $workingDir
     * @param array $options
     *
     * @return string
     */
    private function getTabDelimitedFileExportCommand(
        $database,
        $workingDir,
        array $options
    ) {
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
                $numRows = (int)$this->shellCommandHelper->runShellCommand($numRowsCommand);
                if (0 == $numRows) {
                    continue;
                }

                $getPrimaryKeyCommand = 'mysql ' . escapeshellarg($database) . ' --skip-column-names --silent -e '
                    . '"SELECT \`COLUMN_NAME\` FROM \`information_schema\`.\`COLUMNS\` '
                    . 'WHERE \`TABLE_SCHEMA\` = ' . escapeshellarg($database) . ' '
                    . 'AND \`TABLE_NAME\` = ' . escapeshellarg($table) . ' '
                    . 'AND \`COLUMN_KEY\` = \'PRI\'" '
                    . $this->getMysqlCommandConnectionArguments() . ' ';
                $primaryKeys = trim($this->shellCommandHelper->runShellCommand($getPrimaryKeyCommand));
                if ($primaryKeys) {
                    $orderBy = 'ORDER BY \`' . implode('\`,\`', explode("\n", $primaryKeys)) . '\` ';
                } else {
                    $orderBy = '';
                }

                $numFiles = ceil($numRows / $rowsPerFile);
                $fileNumber = 1;
                for ($i = 0; $i < $numRows; $i += $rowsPerFile) {
                    $dumpDataCommand .= "echo 'Exporting \"$table\" data [$fileNumber/$numFiles]...' 1>&2 && "
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
    private function getMysqlCommandConnectionArguments()
    {
        if ($this->databaseConfig) {
            $connectionArguments = sprintf(
                '-h %s -P %s -u %s -p%s ',
                escapeshellarg($this->databaseConfig->host),
                escapeshellarg($this->databaseConfig->port),
                escapeshellarg($this->databaseConfig->username),
                escapeshellarg($this->databaseConfig->password)
            );
        } else {
            $connectionArguments = '';
        }
        return $connectionArguments;
    }

    /**
     * @param       $database
     * @param array $options
     *
     * @return string
     */
    private function getDumpStructureCommand($database, array $options)
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
    private function prepareWorkingDirectory($path)
    {
        if (!(is_dir($path) && is_writable($path))) {
            throw new Exception\RuntimeException(
                sprintf(
                    'Path "%s" is not a writable directory.',
                    $path
                )
            );
        }

        $workingDir = realpath($path) . '/' . DatabaseExportAdapterInterface::DEFAULT_WORKING_DIR;
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
    private function validateOptions(array $options)
    {
        $validOptionKeys = array_keys($this->getOptionsHelp());
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
    private function getDataTables($database, $options)
    {
        $command = 'mysql --skip-column-names --silent -e "SHOW TABLES from \`' . $database . '\`;" '
            . $this->getMysqlCommandConnectionArguments() . ' ';
        $dataTables = explode("\n", trim($this->shellCommandHelper->runShellCommand($command)));
        if (!empty($options[self::OPTION_IGNORE_TABLES])) {
            $dataTables = array_diff($dataTables, $options[self::OPTION_IGNORE_TABLES]);
        }
        return $dataTables;
    }
}
