<?php

namespace ConductorMySqlSupport\Adapter\TabDelimited;

use ConductorCore\Database\DatabaseImportExportAdapterInterface;
use ConductorCore\Exception;
use ConductorCore\ShellCommandHelper;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ImportPlugin
{
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
     * @var ShellCommandHelper
     */
    private $shellCommandHelper;
    /**
     * @var null|LoggerInterface
     */
    private $logger;


    public function __construct(
        string $username,
        string $password,
        string $host = 'localhost',
        int $port = 3306,
        ShellCommandHelper $shellCommandHelper = null,
        LoggerInterface $logger = null
    ) {
        if (is_null($logger)) {
            $logger = new NullLogger();
        }
        if (is_null($shellCommandHelper)) {
            $shellCommandHelper = new ShellCommandHelper($logger);
        }
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
        $this->shellCommandHelper = $shellCommandHelper;
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
    public function importFromFile(
        string $filename,
        string $database,
        array $options = []
    ): void {
        $this->logger->info("Importing file $filename into database $database");
        $this->assertIsUsable();
        $this->validateOptions($options);
        $extractedDir = $this->extractAndValidateImportFile($filename);

        $command = $this->getTabDelimitedFileImportCommand($database, $extractedDir);

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
            $this->shellCommandHelper->runShellCommand('rm -rf ' . escapeshellarg($extractedDir));
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }
    }

    /**
     * @inheritdoc
     */
    public function setLogger(LoggerInterface $logger): void
    {
        $this->shellCommandHelper->setLogger($logger);
        $this->logger = $logger;
    }

    /**
     * @param string $database
     * @param string $extractedDir
     *
     * @return string
     */
    private function getTabDelimitedFileImportCommand(string $database, string $extractedDir): string
    {
        $importSchemaCommand = 'mysql ' . escapeshellarg($database) . ' '
            . $this->getMysqlCommandConnectionArguments() . ' '
            . "< $extractedDir/schema.sql";

        $dataFiles = glob("$extractedDir/*.txt");
        if ($dataFiles) {
            $importDataCommand = 'mysqlimport ' . escapeshellarg($database) . ' --local --verbose '
                . $this->getMysqlCommandConnectionArguments() . ' '
                . implode(' ', $dataFiles);
        }

        $command = $importSchemaCommand;
        if (!empty($importDataCommand)) {
            $command .= " && $importDataCommand";
        }
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
     * @param string $filename
     *
     * @throws Exception\RuntimeException If file extension or format invalid
     * @return string Extracted directory path
     */
    private function extractAndValidateImportFile(string $filename): string
    {
        if (0 != strcasecmp('.tgz', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .tgz.');
        }

        $path = realpath(dirname($filename));
        $this->shellCommandHelper->runShellCommand(
            'cd ' . escapeshellarg($path)
            . ' && tar xzf ' . escapeshellarg(basename($filename))
        );
        $extractedDir = "$path/" . DatabaseImportExportAdapterInterface::DEFAULT_WORKING_DIR;

        if (!is_dir($extractedDir)) {
            throw new Exception\RuntimeException(
                'Provided file is not a database export created by conductor database:export.'
            );
        }

        return $extractedDir;
    }

    /**
     * @param array $options
     *
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        if ($options) {
            throw new Exception\DomainException(__CLASS__ . ' does not currently support any options.');
        }
    }
}
