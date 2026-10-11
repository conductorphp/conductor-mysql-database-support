<?php

namespace ConductorMySqlSupport\Adapter\TabDelimited;

use ConductorMySqlSupport\Adapter\ClientCredentials;
use ConductorMySqlSupport\Adapter\TlsOptions;
use ConductorCore\Database\DatabaseImportExportAdapterInterface;
use ConductorCore\Exception;
use ConductorCore\Runtime\Requirement;
use ConductorCore\Runtime\RequirementChecker;
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

        $this->credentials = new ClientCredentials($username, $password, $host, $port);
        $this->shellAdapter = $shellAdapter;
        $this->logger = $logger;
    }

    public function assertIsUsable(): void
    {
        (new RequirementChecker($this->shellAdapter))->assert(self::class, $this->runtimeRequirements());
    }

    /**
     * The commands this plugin shells out to, checked when the adapter is selected by configuration
     * and again before each run (CTAP-1759).
     *
     * @return list<Requirement>
     */
    public function runtimeRequirements(): array
    {
        return [
            Requirement::binary('mysql', 'the MariaDB or MySQL client'),
            Requirement::binary('mysqlimport', 'the MariaDB or MySQL client'),
            Requirement::binary('tar'),
        ];
    }

    public function importFromFile(
        string $filename,
        string $database,
        array  $options = []
    ): void {
        $this->logger->info("Importing file $filename into database $database");
        $this->assertIsUsable();
        $this->validateOptions($options);
        $extractedDir = $this->extractAndValidateImportFile($filename);

        $command = $this->getTabDelimitedFileImportCommand($database, $extractedDir);

        try {
            $this->shellAdapter->runShellCommand(
                $command,
                null,
                $this->credentials->environment(),
                ShellAdapterInterface::PRIORITY_LOW
            );
            $this->shellAdapter->runShellCommand('rm -rf ' . escapeshellarg($extractedDir));
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }
    }

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

    private function getMysqlCommandConnectionArguments(): string
    {
        // The password is not among them: it reaches the client as MYSQL_PWD (CTAP-2218).
        return $this->credentials->connectionArguments() . $this->tls->mysqlClientArguments();
    }

    /**
     * @return string Extracted directory path
     * @throws Exception\RuntimeException If file extension or format invalid
     */
    private function extractAndValidateImportFile(string $filename): string
    {
        if (0 !== strcasecmp('.tgz', substr($filename, -4))) {
            throw new Exception\RuntimeException('Invalid file extension. Should be .tgz.');
        }

        $path = realpath(dirname($filename));
        $this->shellAdapter->runShellCommand(
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
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        if ($options) {
            throw new Exception\DomainException(__CLASS__ . ' does not currently support any options.');
        }
    }

    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }
}
