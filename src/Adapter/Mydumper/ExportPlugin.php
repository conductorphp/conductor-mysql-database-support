<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Database\DatabaseImportExportAdapterInterface;
use ConductorCore\ShellCommandHelper;
use ConductorMySqlSupport\Exception;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ExportPlugin
{
    const OPTION_IGNORE_TABLES = 'ignore_tables';
    const OPTION_REMOVE_DEFINERS = 'remove_definers';

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
    public function exportToFile(
        string $database,
        string $path,
        array $options = []
    ): string {
        $workingDir = $this->prepareWorkingDirectory($path);
        $path = realpath($path);
        $this->logger->info("Exporting database $database to file $path/$database.tgz");

        $this->assertIsUsable();
        $this->validateOptions($options);

        $command = $this->getMyDumperExportCommand($database, $workingDir, $options);

        try {
            $this->shellCommandHelper->runShellCommand($command, ShellCommandHelper::PRIORITY_LOW);
            $this->shellCommandHelper->runShellCommand('rm -rf ' . escapeshellarg($workingDir));

        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage());
        }

        return "$path/$database.tgz";
    }

    /**
     * @param string $database
     * @param string $workingDir
     * @param array  $options
     *
     * @return string
     */
    private function getMyDumperExportCommand(string $database, string $workingDir, array $options): string
    {
        # find command has a bug where it will fail if you do not have read permissions to the current working directory
        # Temporarily switching into the working directory while running this command.
        # @link https://unix.stackexchange.com/questions/349894/can-i-tell-find-to-to-not-restore-initial-working-directory

        $dumpStructureCommand = 'mydumper --database ' . escapeshellarg($database) . ' --outputdir '
            . escapeshellarg($workingDir) . ' -v 3 --no-data --triggers --events --routines --less-locking '
            . $this->getMysqldumperCommandConnectionArguments() . ' ';

        if (!empty($options[self::OPTION_REMOVE_DEFINERS])) {
            $dumpStructureCommand .= '&& cd ' . escapeshellarg($workingDir)
                . ' && find . -name "*-schema-triggers.sql" -exec sed -ri \'s|DEFINER=[^ ]+ *||g\' {} \;';
        }

        $dumpDataCommand = 'mydumper --database ' . escapeshellarg($database) . ' --outputdir '
            . escapeshellarg($workingDir) . ' -v 3 --no-schemas --less-locking '
            . $this->getMysqldumperCommandConnectionArguments() . ' ';

        $dataTables = $this->getDataTables($database, $options);
        if ($dataTables) {
            $dumpDataCommand .= '--tables-list ' . implode(',', $dataTables);
        }

        // Avoid issue with tables that defaults timestamp fields to '0000-00-00 00:00:00', which cause on error on
        // import
        $fixTimestampDefaultIssueCommand = 'cd ' . escapeshellarg($workingDir)
            . ' && find . -name "*.sql" -exec sed -ri "s|(timestamp\|datetime) (NOT )?NULL DEFAULT '
            . '\'0000-00-00 00:00:00\'|\1 \2NULL DEFAULT CURRENT_TIMESTAMP|gI" {} \;';

        $tarCommand = 'cd ' . escapeshellarg(dirname($workingDir)) . ' && '
            . 'tar czf ' . escapeshellarg("$database.tgz") . ' ' . escapeshellarg(basename($workingDir));

        return "$dumpStructureCommand && $dumpDataCommand && $fixTimestampDefaultIssueCommand && $tarCommand";
    }

    /**
     * @return string
     */
    private function getMysqldumperCommandConnectionArguments(): string
    {
        return sprintf(
            '-h %s -P %s -u %s -p %s ',
            escapeshellarg($this->host),
            escapeshellarg($this->port),
            escapeshellarg($this->username),
            escapeshellarg($this->password)
        );
    }

    /**
     * @param string $database
     * @param array  $options
     *
     * @return array
     */
    private function getDataTables(string $database, array $options): array
    {
        $dataTables = [];
        if (!empty($options[self::OPTION_IGNORE_TABLES])) {
            $command = 'mysql --skip-column-names --silent -e "SHOW TABLES from \`' . $database . '\`;" '
                . $this->getMysqlCommandConnectionArguments() . ' ';
            $allTables = explode("\n", trim($this->shellCommandHelper->runShellCommand($command)));
            $dataTables = array_diff($allTables, $options[self::OPTION_IGNORE_TABLES]);
        }
        return $dataTables;
    }

    /**
     * @param string $path
     *
     * @throws Exception\RuntimeException If path is not writable
     * @return string Working directory
     */
    private function prepareWorkingDirectory(string $path): string
    {
        if (!(is_dir($path) && is_writable($path))) {
            throw new Exception\RuntimeException(
                sprintf(
                    'Path "%s" is not a writable directory.',
                    $path
                )
            );
        }

        $workingDir = realpath($path) . '/' . DatabaseImportExportAdapterInterface::DEFAULT_WORKING_DIR;
        if (!is_dir($workingDir)) {
            mkdir($workingDir);
        }
        return $workingDir;
    }

    /**
     * @param array $options
     *
     * @throws Exception\DomainException If invalid options provided
     */
    private function validateOptions(array $options): void
    {
        $validOptionKeys = [self::OPTION_IGNORE_TABLES, self::OPTION_REMOVE_DEFINERS];
        $invalidOptionKeys = array_diff(array_keys($options), $validOptionKeys);
        if ($invalidOptionKeys) {
            throw new Exception\DomainException('Invalid options ' . implode(', ', $invalidOptionKeys) . ' provided.');
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
                'mysql',
                'mydumper',
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
