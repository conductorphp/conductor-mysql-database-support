<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Database\DatabaseImportExportAdapterInterface;
use ConductorCore\ShellCommandHelper;
use ConductorMySqlSupport\Exception;
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
    public function importFromFile(
        string $filename,
        string $database,
        array $options = []
    ): void {
        $this->logger->info("Importing file $filename into database $database");
        $this->assertIsUsable();
        $this->validateOptions($options);
        $extractedPath = $this->extractAndValidateImportFile($filename, $database);
        $command = $this->getMyDumperImportCommand($database, $extractedPath);

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
            $this->shellCommandHelper->runShellCommand('rm -rf ' . escapeshellarg($extractedPath));
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }
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


    /**
     * @param string $database
     * @param string $importDir
     *
     * @return string
     */
    private function getMyDumperImportCommand(string $database, string $importDir): string
    {
        $importCommand = 'myloader --database ' . escapeshellarg($database) . ' --directory '
            . escapeshellarg($importDir) . ' -v 3 --overwrite-tables '
            . $this->getMysqlCommandConnectionArguments();

        return $importCommand;
    }

    /**
     * @return string
     */
    private function getMysqlCommandConnectionArguments(): string
    {
        // @todo Consider space after -p. I think for the mysql command it should be left off, but for mydumper it's needed
        return sprintf(
            '-h %s -P %s -u %s -p %s ',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            escapeshellarg($this->password)
        );
    }


    /**
     * @param string $filename
     * @param string $database
     *
     * @throws Exception\RuntimeException If file extension or format invalid
     * @return string Extracted directory path
     */
    private function extractAndValidateImportFile(string $filename, string $database): string
    {
        if (0 != strcasecmp('.tgz', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .tgz.');
        }

        $path = realpath(dirname($filename));
        $this->shellCommandHelper->runShellCommand(
            'cd ' . escapeshellarg($path)
            . ' && tar xzf ' . escapeshellarg(basename($filename))
        );

        if (!file_exists("$path/$database")) {
            throw new Exception\RuntimeException("File \"$filename\" is not a valid mydumper export.");
        }

        return "$path/$database";
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger): void
    {
        $this->shellCommandHelper->setLogger($logger);
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
                'tar',
                'myloader',
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
}
