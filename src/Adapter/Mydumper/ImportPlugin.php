<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Exception;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ImportPlugin
{
    private string $username;
    private string $password;
    private string $host;
    private int $port;
    private ShellAdapterInterface $shellAdapter;
    private LoggerInterface $logger;


    public function __construct(
        ShellAdapterInterface $shellAdapter,
        string                $username,
        string                $password,
        string                $host = 'localhost',
        int                   $port = 3306,
        ?LoggerInterface      $logger = null
    ) {
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
        $this->shellAdapter = $shellAdapter;
        if (is_null($logger)) {
            $logger = new NullLogger();
        }
        $this->logger = $logger;
    }

    public function importFromFile(
        string $filename,
        string $database,
        array  $options = []
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
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        if ($options) {
            throw new Exception\DomainException(__CLASS__ . ' does not currently support any options.');
        }
    }


    private function getMyDumperImportCommand(string $database, string $importDir): string
    {
        return 'myloader --database ' . escapeshellarg($database) . ' --directory '
            . escapeshellarg($importDir) . ' -v 3 --overwrite-tables '
            . $this->getMysqlCommandConnectionArguments();
    }

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
            . ' && rm -f ' . escapeshellarg(basename($filename))
        );

        $files = array_slice(scandir($path), 2);
        if (1 !== count($files)) {
            throw new Exception\RuntimeException("File \"$filename\" is not a valid mydumper export.");
        }

        $extractedPath = "$path/{$files[0]}";

        // Fix metadata file if it was created by an old version of mydumper
        $this->fixMetadataFile($extractedPath);

        return $extractedPath;
    }

    /**
     * Fix metadata file from old mydumper versions that don't include the [config] group header
     */
    private function fixMetadataFile(string $extractedPath): void
    {
        $metadataFile = $extractedPath . '/metadata';

        if (!file_exists($metadataFile)) {
            $this->logger->warning("Metadata file not found at $metadataFile");
            return;
        }

        $content = file_get_contents($metadataFile);
        if ($content === false) {
            $this->logger->warning("Failed to read metadata file at $metadataFile");
            return;
        }

        // Check if the file already starts with a group header
        $trimmedContent = ltrim($content);
        if (preg_match('/^\[.*?\]/', $trimmedContent)) {
            // File already has a group header, no fix needed
            return;
        }

        // Add [config] group header at the top
        $fixedContent = "[config]\n" . $content;

        if (file_put_contents($metadataFile, $fixedContent) === false) {
            $this->logger->warning("Failed to fix metadata file at $metadataFile");
            return;
        }

        $this->logger->info("Fixed metadata file from old mydumper version by adding [config] header");
    }

    public function assertIsUsable(): void
    {
        try {
            if (!is_callable('exec')) {
                throw new Exception\RuntimeException('the "exec" function is not callable.');
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

    public function setLogger(LoggerInterface $logger): void
    {
        if ($this->shellAdapter instanceof LoggerAwareInterface) {
            $this->shellAdapter->setLogger($logger);
        }
        $this->logger = $logger;
    }
}
