<?php

namespace DevopsToolMySqlSupport\Adapter\TabDelimited;

use DevopsToolCore\Database\DatabaseExportAdapterInterface;
use DevopsToolCore\Database\DatabaseImportAdapterInterface;
use DevopsToolCore\Exception;
use DevopsToolCore\ShellCommandHelper;
use DevopsToolMySqlSupport\Adapter\DatabaseConfig;
use Monolog\Handler\NullHandler;
use Psr\Log\LoggerInterface;

class MysqlTabDelimitedImportAdapter implements DatabaseImportAdapterInterface
{
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
     * MydumperExportAdapter constructor.
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
                'mysql',
                'mysqlimport',
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
        return [];
    }

    /**
     * @inheritdoc
     */
    public function importFromFile(
        $filename,
        $database,
        array $options = []
    ) {
        $this->assertIsUsable();
        $this->validateOptions($options);
        $extractedDir = $this->extractAndValidateImportFile($filename);

        $command = $this->getTabDelimitedFileImportCommand($database, $extractedDir, $options);

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
            $this->shellCommandHelper->runShellCommand('rm -rf ' . escapeshellarg($extractedDir));
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }
        return $filename;
    }

    /**
     * @inheritdoc
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

    private function getTabDelimitedFileImportCommand($database, $extractedDir, array $options)
    {
        $importSchemaCommand = 'mysql ' . escapeshellarg($database) . ' '
            . $this->getMysqlCommandConnectionArguments() . ' '
            . "< $extractedDir/schema.sql";

        $dataFiles = glob("$extractedDir/*.txt");
        $importDataCommand = 'mysqlimport ' . escapeshellarg($database) . ' --local --verbose '
            . $this->getMysqlCommandConnectionArguments() . ' '
            . implode(' ', $dataFiles);

        return "$importSchemaCommand && $importDataCommand";
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
                escapeshellarg($this->databaseConfig->user),
                escapeshellarg($this->databaseConfig->password)
            );
        } else {
            $connectionArguments = '';
        }
        return $connectionArguments;
    }

    /**
     * @param string $filename
     *
     * @throws Exception\RuntimeException If file extension or format invalid
     * @return string Extracted directory path
     */
    private function extractAndValidateImportFile($filename)
    {
        if (0 != strcasecmp('.tgz', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .tgz.');
        }

        $path = realpath(dirname($filename));
        $this->shellCommandHelper->runShellCommand(
            'cd ' . escapeshellarg($path)
            . ' && tar xzf ' . escapeshellarg(basename($filename))
        );
        $extractedDir = "$path/" . DatabaseExportAdapterInterface::DEFAULT_WORKING_DIR;

        if (!is_dir($extractedDir)) {
            throw new Exception\RuntimeException(
                'Provided file is not a database export created by devops database:export.'
            );
        }

        return $extractedDir;
    }

    /**
     * @param array $options
     *
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options)
    {
        if ($options) {
            throw new Exception\DomainException(__CLASS__ . ' does not currently support any options.');
        }
    }
}
