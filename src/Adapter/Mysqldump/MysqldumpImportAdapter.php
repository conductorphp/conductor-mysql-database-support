<?php

namespace DevopsToolMySqlSupport\Adapter\Mysqldump;

use DevopsToolCore\Database\DatabaseExportAdapterInterface;
use DevopsToolCore\Database\DatabaseImportAdapterInterface;
use DevopsToolCore\Exception;
use DevopsToolCore\ShellCommandHelper;
use DevopsToolMySqlSupport\Adapter\DatabaseConfig;
use Monolog\Handler\NullHandler;
use Psr\Log\LoggerInterface;

class MysqldumpImportAdapter //implements DatabaseImportAdapterInterface
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
     * MysqldumpImportAdapter constructor.
     *
     * @param DatabaseConfig       $databaseConfig
     * @param ShellCommandHelper   $shellCommandHelper
     * @param LoggerInterface|null $logger
     */
    public function __construct(
        DatabaseConfig $databaseConfig,
        ShellCommandHelper $shellCommandHelper,
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
                'gunzip',
                'mysql',
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
            throw new Exception\RuntimeException(sprintf(
                __CLASS__
                . ' is not usable in this environment because ' . $e->getMessage()
            ));
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
        $filename = $this->extractAndValidateImportFile($filename);

        $command = 'mysql ' . escapeshellarg($database) . ' '
            . $this->getMysqlCommandConnectionArguments()
            . ' < ' . escapeshellarg($filename);

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
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

    /**
     * @param string $filename
     *
     * @throws Exception\RuntimeException If file extension or format invalid
     * @return string Extracted filename
     */
    private function extractAndValidateImportFile($filename)
    {
        if (0 != strcasecmp('.sql.gz', substr($filename, -7)) && 0 == strcasecmp('.sql', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .sql or .sql.gz.');
        }

        $filename = realpath($filename);
        // Extract gzip if needed
        if (0 == strcasecmp('.sql.gz', substr($filename, -7))) {
            $this->shellCommandHelper->runShellCommand('gunzip ' . escapeshellarg($filename));
            $filename = substr($filename, 0, strlen($filename) - 3);
        }
        return $filename;
    }

}
