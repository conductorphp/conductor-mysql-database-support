<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Adapter\Mydumper\ImportPlugin;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

class ImportPluginTest extends TestCase
{
    /**
     * mydumper 0.19 opens metadata with a comment and only then writes its groups.
     */
    private const MODERN_METADATA = <<<METADATA
        # Started dump at: 2026-08-31 21:44:48
        [config]
        quote-character = BACKTICK

        [myloader_session_variables]
        SQL_MODE='NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION' /*!40101


        [`middleware`.`product`]
        real_table_name=product
        rows = 2
        # Finished dump at: 2026-08-31 21:44:48

        METADATA;

    /**
     * Old mydumper wrote prose with no groups at all. myloader needs it converted to key=value.
     */
    private const LEGACY_METADATA = <<<METADATA
        Started dump at: 2025-11-12 09:00:08
        Finished dump at: 2025-11-12 09:00:08

        METADATA;

    private string $dumpDir;

    public function setUp(): void
    {
        $this->dumpDir = sys_get_temp_dir() . '/mydumper-import-' . bin2hex(random_bytes(6));
        mkdir($this->dumpDir);
    }

    public function tearDown(): void
    {
        foreach (glob($this->dumpDir . '/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($this->dumpDir);
    }

    /**
     * The conversion drops every line that is neither a comment nor a key=value pair, so running it
     * over a modern metadata file strips all of its group headers and leaves myloader a file it cannot
     * use. A group header anywhere means the file is already in the format myloader wants.
     */
    public function testLeavesModernMetadataAlone(): void
    {
        file_put_contents($this->dumpDir . '/metadata', self::MODERN_METADATA);

        $this->fixMetadataFile();

        $this->assertSame(self::MODERN_METADATA, file_get_contents($this->dumpDir . '/metadata'));
    }

    public function testStillConvertsLegacyMetadata(): void
    {
        file_put_contents($this->dumpDir . '/metadata', self::LEGACY_METADATA);

        $this->fixMetadataFile();

        $this->assertSame(
            "[snapshot]\nstarted=2025-11-12 09:00:08\nfinished=2025-11-12 09:00:08\n",
            file_get_contents($this->dumpDir . '/metadata')
        );
    }

    public function testReportsTheErrorsMyLoaderActuallyDiedOn(): void
    {
        $errorLog = $this->dumpDir . '/myloader.log';
        file_put_contents($errorLog, <<<LOG
            ** Message: 21:45:45.604: S-Thread 7: Starting import
            (myloader:36): GLib-CRITICAL **: 21:45:45.604: g_key_file_get_groups: assertion 'key_file != NULL' failed
            ** (myloader:36): WARNING **: 21:45:45.612: Thread 5 using connection 39 - ERROR 1101: BLOB, TEXT, GEOMETRY or JSON column 'permissions' can't have a default value
            ** (myloader:36): CRITICAL **: 21:45:45.612: Thread 5 using connection 39 - ERROR 1101: BLOB, TEXT, GEOMETRY or JSON column 'permissions' can't have a default value
            ** (myloader:36): CRITICAL **: 21:45:45.612: Thread 5: issue restoring /tmp/dump/middleware.product-schema.sql

            LOG);

        $errors = $this->getMyLoaderErrors($errorLog);

        $this->assertSame(
            [
                "Thread 5 using connection 39 - ERROR 1101: BLOB, TEXT, GEOMETRY or JSON column "
                . "'permissions' can't have a default value",
                'Thread 5: issue restoring /tmp/dump/middleware.product-schema.sql',
            ],
            $errors
        );
    }

    public function testReportsNothingWhenMyLoaderLeftNoLog(): void
    {
        $this->assertSame([], $this->getMyLoaderErrors($this->dumpDir . '/does-not-exist.log'));
    }

    /**
     * mydumper 1.0 renamed --overwrite-tables to -o/--drop-table. The mode is passed with = rather
     * than as a separate token because the option's argument is optional, so a space-separated value
     * is not bound to it reliably.
     */
    public function testDropsExistingTablesUsingTheMydumper1Option(): void
    {
        $command = $this->getMyDumperImportCommand('mydb', '/dump', '/dump.log');

        $this->assertStringContainsString('--drop-table=DROP', $command);
        $this->assertStringNotContainsString('--overwrite-tables', $command);
        $this->assertStringNotContainsString('--purge-mode', $command);
    }

    /**
     * The error log is captured through tee rather than a plain redirect so myloader's progress still
     * streams to the shell adapter while a copy lands on disk for the failure message.
     */
    public function testCapturesMyLoaderStderrWithoutSwallowingIt(): void
    {
        $command = $this->getMyDumperImportCommand('mydb', '/dump', '/dump.log');

        $this->assertStringContainsString("2> >(tee '/dump.log' >&2)", $command);
    }

    private function getMyDumperImportCommand(string $database, string $importDir, string $errorLog): string
    {
        $method = new ReflectionMethod(ImportPlugin::class, 'getMyDumperImportCommand');

        return $method->invoke($this->createImportPlugin(), $database, $importDir, $errorLog);
    }

    private function fixMetadataFile(): void
    {
        $method = new ReflectionMethod(ImportPlugin::class, 'fixMetadataFile');
        $method->invoke($this->createImportPlugin(), $this->dumpDir);
    }

    /**
     * @return string[]
     */
    private function getMyLoaderErrors(string $errorLog): array
    {
        $method = new ReflectionMethod(ImportPlugin::class, 'getMyLoaderErrors');

        return $method->invoke($this->createImportPlugin(), $errorLog);
    }

    private function createImportPlugin(): ImportPlugin
    {
        $shellAdapter = new class implements ShellAdapterInterface {
            public function isCallable(string $command): bool
            {
                return true;
            }

            public function runShellCommand(
                string  $command,
                ?string $currentWorkingDirectory = null,
                ?array  $environmentVariables = null,
                int     $priority = self::PRIORITY_NORMAL,
                ?array  $options = null
            ): string {
                throw new \LogicException('No shell command should be run by these tests.');
            }
        };

        return new ImportPlugin($shellAdapter, 'username', 'password');
    }
}
