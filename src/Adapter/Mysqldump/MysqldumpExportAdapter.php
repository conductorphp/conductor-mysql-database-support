<?php

namespace DevopsToolMySqlSupport\Adapter\Mysqldump;

use DevopsToolCore\Database\DatabaseExportAdapterInterface;
use DevopsToolCore\Exception;
use DevopsToolCore\ShellCommandHelper;
use DevopsToolMySqlSupport\Adapter\DatabaseConfig;
use Monolog\Handler\NullHandler;
use Psr\Log\LoggerInterface;

class MysqldumpExportAdapter implements DatabaseExportAdapterInterface
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

    public function __construct(
        DatabaseConfig $databaseConfig,
        ShellCommandHelper $shellCommandHelper,
        LoggerInterface $logger = null
    ) {
        if (!$this->isUsable()) {
            throw new Exception\RuntimeException(__CLASS__ . ' is not usable in this environment.');
        }

        $this->databaseConfig = $databaseConfig;
        $shellCommandHelper = new ShellCommandHelper();
        $this->shellCommandHelper = $shellCommandHelper;
        if (is_null($logger)) {
            $logger = new NullHandler();
        }
        $this->logger = $logger;
    }

    /**
     * @inheritdoc
     */
    public function isUsable()
    {
        $usedFunctions = [
            'gzip',
            'gunzip',
            'mysql',
            'mysqldump',
        ];
        exec('which ' . implode(' &> /dev/null && which ', $usedFunctions) . ' &> /dev/null', $output, $return);
        return (0 == $return);
    }

    /**
     * @inheritdoc
     */
    public function getFileExtension()
    {
        return 'sql.gz';
    }

    /**
     * @param            $database
     * @param array      $ignoreTables
     * @param string     $filename
     * @param bool       $removeDefiners
     *
     * @throws Exception\RuntimeException If command fails
     * @return string filename
     */
    public function exportToFile(
        $database,
        $filename,
        array $ignoreTables = [],
        $removeDefiners = true
    ) {
        $filename = $this->normalizeFilename($filename);
        $path = dirname($filename);
        if (!(is_dir($path) && is_writable($path))) {
            throw new Exception\RuntimeException(
                sprintf(
                    'Path "%s" is not a writable directory.',
                    $path
                )
            );
        }

        $dumpStructureCommand = $this->getDumpStructureCommand($database, $removeDefiners);
        $dumpDataCommand = 'mysqldump ' . escapeshellarg($database) . ' '
            . $this->getCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false '
            . '--order-by-primary --skip-comments --no-create-db --no-create-info --skip-triggers ';
        if ($ignoreTables) {
            foreach ($ignoreTables as $table) {
                $dumpDataCommand .= '--ignore-table=' . escapeshellarg("$database.$table") . ' ';
            }
        }

        $command = "($dumpStructureCommand && $dumpDataCommand) "
            . '| gzip -9 > ' . escapeshellarg($filename);

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }
        return $filename;
    }

    /**
     * @return string
     */
    private function getCommandConnectionArguments()
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
     * @param $database
     * @param $removeDefiners
     *
     * @return string
     */
    private function getDumpStructureCommand($database, $removeDefiners)
    {
        $dumpStructureCommand = 'mysqldump ' . escapeshellarg($database) . ' '
            . $this->getCommandConnectionArguments() . ' '
            . '--single-transaction --quick --lock-tables=false --skip-comments --no-data --verbose ';
        if ($removeDefiners) {
            $dumpStructureCommand .= '| sed "s/DEFINER=[^*]*\*/\*/g" ';
        }
        return $dumpStructureCommand;
    }

    /**
     * @param $filename
     *
     * @return string
     */
    private function normalizeFilename($filename)
    {
        // Normalize filename
        if (!preg_match('%^\.{0,2}/%', $filename)) {
            $filename = './' . $filename;
        }

        // Normalize file extension
        $ext = self::getFileExtension();
        if (".$ext" != substr($filename, -(strlen($ext) + 1))) {
            $filename .= ".$ext";
        }
        return $filename;
    }
}
