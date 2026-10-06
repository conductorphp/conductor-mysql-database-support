<?php

namespace ConductorMySqlSupport\Adapter\Mysqldump;

use ConductorMySqlSupport\Adapter\ClientCredentials;
use ConductorMySqlSupport\Adapter\TlsOptions;
use ConductorCore\Exception;
use ConductorCore\Shell\Adapter\LocalShellAdapter;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ImportPlugin
{
    private ClientCredentials $credentials;
    private ShellAdapterInterface $shellAdapter;
    private LoggerInterface $logger;


    private TlsOptions $tls;

    public function __construct(
        ShellAdapterInterface $shellAdapter,
        string                $username,
        string                $password,
        string                $host = 'localhost',
        int                   $port = 3306,
        ?LoggerInterface      $logger = null,
        ?TlsOptions $tls = null
    ) {
        $this->tls = $tls ?? TlsOptions::disabled();
        if (is_null($logger)) {
            $logger = new NullLogger();
        }
        if (is_null($shellAdapter)) {
            $shellAdapter = new LocalShellAdapter($logger);
        }
        $this->credentials = new ClientCredentials($username, $password, $host, $port);
        $this->shellAdapter = $shellAdapter;
        $this->logger = $logger;
    }

    public function assertIsUsable(): void
    {
        try {
            if (!is_callable('exec')) {
                throw new Exception\RuntimeException('the "exec" function is not callable.');
            }

            $requiredFunctions = [
                'gunzip',
                'mysql',
            ];
            $missingFunctions = [];
            foreach ($requiredFunctions as $requiredFunction) {
                exec('which ' . escapeshellarg($requiredFunction) . ' &> /dev/null', $output, $return);
                if (0 !== $return) {
                    $missingFunctions[] = $requiredFunction;
                }
            }

            if ($missingFunctions) {
                throw new Exception\RuntimeException(
                    sprintf(
                        'the "%s" shell function(s) are not available.',
                        implode('", "', $missingFunctions)
                    )
                );
            }
        } catch (\Exception $e) {
            throw new Exception\RuntimeException(
                sprintf(
                    '%s is not usable in this environment because %s.',
                    __CLASS__,
                    $e->getMessage()
                )
            );
        }
    }

    public function importFromFile(
        string $filename,
        string $database,
        array  $options = []
    ): void {
        $this->logger->info("Importing file $filename into database $database");
        $this->assertIsUsable();
        $this->validateOptions($options);
        $filename = $this->extractAndValidateImportFile($filename);

        $command = 'mysql ' . escapeshellarg($database) . ' '
            . $this->getMysqlCommandConnectionArguments()
            . ' < ' . escapeshellarg($filename);

        try {
            $this->shellAdapter->runShellCommand(
                $command,
                null,
                $this->credentials->environment(),
                ShellAdapterInterface::PRIORITY_LOW
            );
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }
    }

    private function getMysqlCommandConnectionArguments(): string
    {
        // The password is not among them: it reaches the client as MYSQL_PWD (CTAP-2218).
        return $this->credentials->connectionArguments() . $this->tls->mysqlClientArguments();
    }

    /**
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        if ($options) {
            throw new Exception\DomainException(__CLASS__ . ' does not currently support any options.');
        }
    }

    /**
     * @return string Extracted filename
     * @throws Exception\RuntimeException If file extension or format invalid
     */
    private function extractAndValidateImportFile(string $filename): string
    {
        if (0 !== strcasecmp('.sql.gz', substr($filename, -7)) && 0 != strcasecmp('.sql', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .sql or .sql.gz.');
        }

        $path = realpath($filename);
        if (false === $path) {
            // realpath() is false for a missing file, which used to become an empty input redirect
            // and a confusing shell failure.
            throw new Exception\RuntimeException(sprintf('Import file "%s" does not exist.', $filename));
        }
        $filename = $path;
        // Extract gzip if needed
        if (0 === strcasecmp('.sql.gz', substr($filename, -7))) {
            $this->shellAdapter->runShellCommand('gunzip -f ' . escapeshellarg($filename));
            $filename = substr($filename, 0, -3);
        }
        return $filename;
    }

    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }

}
