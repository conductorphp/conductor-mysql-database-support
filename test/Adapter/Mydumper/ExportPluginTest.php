<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Adapter\Mydumper\ExportPlugin;
use ConductorMySqlSupport\Exception;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

class ExportPluginTest extends TestCase
{
    /**
     * What `tar -tzf` prints for an archive myloader can restore.
     */
    private const ARCHIVE_LISTING = <<<LISTING
        mydb/
        mydb/metadata
        mydb/mydb.product-schema.sql
        mydb/mydb.product.00000.sql

        LISTING;

    private string $workingDir;

    public function setUp(): void
    {
        $this->workingDir = sys_get_temp_dir() . '/mydumper-export-' . bin2hex(random_bytes(6));
        mkdir($this->workingDir);
    }

    public function tearDown(): void
    {
        foreach (glob($this->workingDir . '/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($this->workingDir);
    }

    public function testAcceptsAnArchiveMyLoaderCouldRestore(): void
    {
        $this->expectNotToPerformAssertions();
        $archive = $this->writeArchive('not empty');

        $this->assertArchiveIsRestorable($archive, self::ARCHIVE_LISTING);
    }

    /**
     * mydumper exiting 0 is not proof it wrote anything. Without this, the caller is handed a path to
     * a file that does not exist and finds out at restore time.
     */
    public function testRefusesToReportAnExportThatWroteNoArchive(): void
    {
        $archive = $this->workingDir . '/mydb.tgz';

        $this->expectException(Exception\RuntimeException::class);
        $this->expectExceptionMessage('wrote no archive');

        $this->assertArchiveIsRestorable($archive, self::ARCHIVE_LISTING);
    }

    public function testRefusesToReportAnEmptyArchive(): void
    {
        $archive = $this->writeArchive('');

        $this->expectException(Exception\RuntimeException::class);
        $this->expectExceptionMessage('wrote an empty archive');

        $this->assertArchiveIsRestorable($archive, self::ARCHIVE_LISTING);
    }

    /**
     * myloader refuses a directory with no metadata file, so an archive without one is not a snapshot
     * however well formed the tar is.
     */
    public function testRefusesAnArchiveWithNoMetadataEntry(): void
    {
        $archive = $this->writeArchive('not empty');

        $this->expectException(Exception\RuntimeException::class);
        $this->expectExceptionMessage('contains no metadata file');

        $this->assertArchiveIsRestorable($archive, "mydb/\nmydb/mydb.product-schema.sql\n");
    }

    public function testSaysWhereTheWorkingDirectoryWasLeftWhenTheExportIsRefused(): void
    {
        $archive = $this->writeArchive('');

        $this->expectException(Exception\RuntimeException::class);
        $this->expectExceptionMessage($this->workingDir);

        $this->assertArchiveIsRestorable($archive, self::ARCHIVE_LISTING);
    }

    /**
     * Every path in the export command is relative, so the command has to be run in the directory the
     * export owns. Left to the process's own working directory, the dump and the archive land wherever
     * conductor was started from while the returned path claims otherwise.
     */
    public function testBuildsAnExportCommandThatIsRelativeToItsWorkingDirectory(): void
    {
        $method = new ReflectionMethod(ExportPlugin::class, 'getMyDumperExportCommand');
        $command = $method->invoke($this->createExportPlugin(''), 'mydb', []);

        $this->assertStringContainsString("--outputdir 'mydb'", $command);
        $this->assertStringContainsString("tar -czf 'mydb.tgz' 'mydb'", $command);
    }

    private function assertArchiveIsRestorable(string $archive, string $archiveListing): void
    {
        $method = new ReflectionMethod(ExportPlugin::class, 'assertArchiveIsRestorable');
        $method->invoke($this->createExportPlugin($archiveListing), $archive, $this->workingDir);
    }

    private function writeArchive(string $contents): string
    {
        $archive = $this->workingDir . '/mydb.tgz';
        file_put_contents($archive, $contents);

        return $archive;
    }

    private function createExportPlugin(string $archiveListing): ExportPlugin
    {
        $shellAdapter = new class ($archiveListing) implements ShellAdapterInterface {
            private string $archiveListing;

            public function __construct(string $archiveListing)
            {
                $this->archiveListing = $archiveListing;
            }

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
                if (str_starts_with($command, 'tar -tzf ')) {
                    return $this->archiveListing;
                }

                throw new \LogicException("These tests run no shell command but tar -tzf. Got: $command");
            }
        };

        return new ExportPlugin($shellAdapter, 'username', 'password');
    }
}
