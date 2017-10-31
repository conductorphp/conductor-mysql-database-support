<?php

namespace DevopsToolMySqlSupport\Adapter\Mysqldump;

use DevopsToolCore\Database\DatabaseExportAdapterInterface;
use DevopsToolCore\Exception;
use DevopsToolCore\ShellCommandHelper;
use DevopsToolMySqlSupport\Adapter\DatabaseConfig;
use Monolog\Handler\NullHandler;
use Psr\Log\LoggerInterface;

class MysqldumpExportAdapter implements DatabaseExportAdapterInterface
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
     * @var array
     */
    private $connectionConfig;

    /**
     * MysqldumpExportAdapter constructor.
     *
     * @param array                $connectionConfig
     * @param ShellCommandHelper   $shellCommandHelper
     * @param LoggerInterface|null $logger
     * @param string|null          $connection
     */
    public function __construct(
        array $connectionConfig,
        ShellCommandHelper $shellCommandHelper,
        LoggerInterface $logger = null,
        $connection = 'default'
    ) {
        $this->connectionConfig = $connectionConfig;
        $this->shellCommandHelper = $shellCommandHelper;
        if (is_null($logger)) {
            $logger = new NullHandler();
        }
        $this->logger = $logger;
        $this->selectConnection($connection);
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
        if (!(is_dir($path) && is_writable($path))) {
            throw new Exception\RuntimeException(
                sprintf(
                    'Path "%s" is not a writable directory.',
                    $path
                )
            );
        }
        $path = realpath($path);

        $dumpStructureCommand = $this->getDumpStructureCommand($database, $options);
        $dumpDataCommand = 'mysqldump ' . escapeshellarg($database) . ' '
            . $this->getCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false '
            . '--order-by-primary --skip-comments --no-create-db --no-create-info --skip-triggers ';
        if (!empty($options[self::OPTION_IGNORE_TABLES])) {
            foreach ($options[self::OPTION_IGNORE_TABLES] as $table) {
                $dumpDataCommand .= '--ignore-table=' . escapeshellarg("$database.$table") . ' ';
            }
        }

        $command = "($dumpStructureCommand && $dumpDataCommand) "
            . '| gzip -9 > ' . escapeshellarg("$path/$database.sql.gz");

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }

        return "$path/$database.sql.gz";
    }

    /**
     * @param LoggerInterface $logger
     *
     * @return void
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->shellCommandHelper->setLogger($logger);
    }

    /**
     * @inheritdoc
     */
    public function selectConnection($name)
    {
        if (!isset($this->connectionConfig[$name])) {
            throw new Exception\DomainException("Connection \"$name\" not provided in connection configuration.");
        }

        $this->databaseConfig = DatabaseConfig::createFromArray($this->connectionConfig[$name]);
    }

    /**
     * @return string
     */
    private function getCommandConnectionArguments()
    {
        if ($this->databaseConfig) {
            $connectionArguments = sprintf(
                '-h %s -P %s -u %s -p%s ',
                escapeshellarg($this->databaseConfig->host),
                escapeshellarg($this->databaseConfig->port),
                escapeshellarg($this->databaseConfig->user),
                escapeshellarg($this->databaseConfig->password)
            );
        } else {
            $connectionArguments = '';
        }
        return $connectionArguments;
    }

    /**
     * @param string $database
     * @param array  $options
     *
     * @return string
     */
    private function getDumpStructureCommand($database, $options)
    {
        $dumpStructureCommand = 'mysqldump ' . escapeshellarg($database) . ' '
            . $this->getCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false --skip-comments --no-data --verbose ';

        if (!empty($options[self::OPTION_REMOVE_DEFINERS])) {
            $dumpStructureCommand .= '| sed "s/DEFINER=[^*]*\*/\*/g" ';
        }

        return $dumpStructureCommand;
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

}
