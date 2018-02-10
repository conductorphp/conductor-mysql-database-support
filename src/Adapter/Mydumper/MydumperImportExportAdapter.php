<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Database\DatabaseImportExportAdapterInterface;
use ConductorCore\Exception;
use ConductorCore\ShellCommandHelper;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class MydumperImportExportAdapter implements DatabaseImportExportAdapterInterface, LoggerAwareInterface
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
        ShellCommandHelper $shellCommandHelper = null,
        ImportPlugin $importPlugin = null,
        ExportPlugin $exportPlugin = null,
        LoggerInterface $logger = null
    ) {
        if (is_null($logger)) {
            $logger = new NullLogger();
        }
        if (is_null($shellCommandHelper)) {
            $shellCommandHelper = new ShellCommandHelper($logger);
        }

        if (is_null($importPlugin)) {
            $importPlugin = new ImportPlugin($username, $password, $host, $port, $shellCommandHelper, $logger);
        }
        $this->importPlugin = $importPlugin;
        if (is_null($exportPlugin)) {
            $exportPlugin = new ExportPlugin($username, $password, $host, $port, $shellCommandHelper, $logger);
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
