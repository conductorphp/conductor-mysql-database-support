<?php

namespace ConductorMySqlSupport\Adapter\TabDelimited;

use ConductorCore\Database\DatabaseImportExportAdapterInterface;
use ConductorCore\Exception;
use ConductorCore\Shell\Adapter\LocalShellAdapter;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class TabDelimitedImportExportAdapter implements DatabaseImportExportAdapterInterface, LoggerAwareInterface
{
    /**
     * @var ImportPlugin
     */
    private $importPlugin;
    /**
     * @var ExportPlugin
     */
    private $exportPlugin;

    public function __construct(
        string $username,
        string $password,
        string $host = 'localhost',
        int $port = 3306,
        ShellAdapterInterface $shellAdapter = null,
        ImportPlugin $importPlugin = null,
        ExportPlugin $exportPlugin = null,
        LoggerInterface $logger = null
    ) {
        if (is_null($logger)) {
            $logger = new NullLogger();
        }
        if (is_null($shellAdapter)) {
            $shellAdapter = new LocalShellAdapter($logger);
        }

        if (is_null($importPlugin)) {
            $importPlugin = new ImportPlugin($shellAdapter, $username, $password, $host, $port, $logger);
        }
        $this->importPlugin = $importPlugin;
        if (is_null($exportPlugin)) {
            $exportPlugin = new ExportPlugin($shellAdapter, $username, $password, $host, $port, $logger);
        }
        $this->exportPlugin = $exportPlugin;
    }

    /**
     * @inheritdoc
     */
    public function importFromFile(
        string $filename,
        string $database,
        array $options = []
    ): void {
        $this->importPlugin->importFromFile($filename, $database, $options);
    }

    /**
     * @inheritdoc
     */
    public function exportToFile(
        string $database,
        string $path,
        array $options = []
    ): string {
        return $this->exportPlugin->exportToFile($database, $path, $options);
    }

    /**
     * @return string
     */
    public static function getFileExtension(): string
    {
        return 'tgz';
    }

    /**
     * @inheritdoc
     */
    public function assertIsUsable(): void
    {
        try {
            $this->importPlugin->assertIsUsable();
            $this->exportPlugin->assertIsUsable();
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
    public function setLogger(LoggerInterface $logger): void
    {
        $this->importPlugin->setLogger($logger);
        $this->exportPlugin->setLogger($logger);
    }
}
