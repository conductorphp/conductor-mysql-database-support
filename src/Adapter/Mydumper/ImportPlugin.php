<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Exception;
use Psr\Log\LoggerAwareInterface;
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
    public function importFromFile(
        string $filename,
        string $database,
        array $options = []
    ): void {
        $this->logger->info("Importing file $filename into database $database");
        $this->assertIsUsable();
        $this->validateOptions($options);
        $extractedPath = $this->extractAndValidateImportFile($filename);
        $command = $this->getMyDumperImportCommand($database, $extractedPath);

        try {
            $this->shellAdapter->runShellCommand($command, null, null, ShellAdapterInterface::PRIORITY_LOW);
            $this->shellAdapter->runShellCommand('rm -rf ' . escapeshellarg($extractedPath));
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
        return sprintf(
            '-h %s -P %s -u %s %s',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            $this->password ? '-p ' . escapeshellarg($this->password) . ' ' : ''
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
        $this->shellAdapter->runShellCommand(
            'cd ' . escapeshellarg($path)
            . ' && tar xzf ' . escapeshellarg(basename($filename))
            . ' && rm -f ' . escapeshellarg(basename($filename))
        );

        $files = array_slice(scandir($path), 2);
        if (1 != count($files)) {
            throw new Exception\RuntimeException("File \"$filename\" is not a valid mydumper export.");
        }

        return "$path/{$files[0]}";
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

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }
}
