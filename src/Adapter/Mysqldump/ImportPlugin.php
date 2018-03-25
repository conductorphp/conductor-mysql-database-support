<?php

namespace ConductorMySqlSupport\Adapter\Mysqldump;

use ConductorCore\Exception;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
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
        if (is_null($shellAdapter)) {
            $shellAdapter = new LocalShellAdapter($logger);
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
        $filename = $this->extractAndValidateImportFile($filename);

        $command = 'mysql ' . escapeshellarg($database) . ' '
            . $this->getMysqlCommandConnectionArguments()
            . ' < ' . escapeshellarg($filename);

        try {
            $this->shellAdapter->runShellCommand($command, null, null,ShellAdapterInterface::PRIORITY_LOW);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }
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
     * @param string $filename
     *
     * @throws Exception\RuntimeException If file extension or format invalid
     * @return string Extracted filename
     */
    private function extractAndValidateImportFile(string $filename): string
    {
        if (0 != strcasecmp('.sql.gz', substr($filename, -7)) && 0 != strcasecmp('.sql', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .sql or .sql.gz.');
        }

        $filename = realpath($filename);
        // Extract gzip if needed
        if (0 == strcasecmp('.sql.gz', substr($filename, -7))) {
            $this->shellAdapter->runShellCommand('gunzip -f ' . escapeshellarg($filename));
            $filename = substr($filename, 0, strlen($filename) - 3);
        }
        return $filename;
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
