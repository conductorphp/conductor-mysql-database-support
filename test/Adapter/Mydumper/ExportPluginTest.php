<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorCore\Exception\ShellCommandFailedException;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Adapter\Mydumper\ExportPlugin;
use ConductorMySqlSupport\Exception;
use PHPUnit\Framework\Attributes\DataProvider;
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

    private const STDERR_WITHOUT_REPLICATION_CLIENT = <<<STDERR
        ** Message: 15:04:55.525: MyDumper backup version: 1.0.5-1

        ** (mydumper:736): WARNING **: 15:04:55.526: Using --trx-tables options, binlog coordinates will not be accurate if you are writing to non transactional tables.
        ** Message: 15:04:55.539: Connected to MySQL 8.4.11
        ** Message: 15:04:55.543: @@tokudb_version not found - ERROR 1193: Unknown system variable 'tokudb_version'

        ** (mydumper:736): WARNING **: 15:04:55.544: Couldn't get master position - ERROR 1227: Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
        ** Message: 15:04:55.547: Thread 3: dumping db information for `mydb`

        ** (mydumper:736): WARNING **: 15:04:55.547: Couldn't get master position - ERROR 1227: Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
        ** Message: 15:04:55.557: Finished dump at: 2026-10-07 15:04:55

        STDERR;

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
        $commands = $this->getMyDumperExportCommands();

        $this->assertStringContainsString("--outputdir 'mydb'", $commands['schema']);
        $this->assertStringContainsString("--outputdir 'mydb'", $commands['data']);
        $this->assertSame("tar -czf 'mydb.tgz' 'mydb'", $commands['archive']);
    }

    /**
     * Each mydumper run is its own command, so its exit status can be judged by itself rather than
     * ending an && chain before the archive is made.
     */
    public function testRunsEachMydumperOnItsOwn(): void
    {
        $commands = $this->getMyDumperExportCommands();

        $this->assertSame(['schema', 'definers', 'data', 'timestamps', 'archive'], array_keys($commands));
        foreach ($commands as $command) {
            $this->assertStringNotContainsString('&&', $command);
        }
        $this->assertStringStartsWith('mydumper ', $commands['schema']);
        $this->assertStringStartsWith('mydumper ', $commands['data']);
    }

    /**
     * A managed database's app user has no REPLICATION CLIENT. mydumper still writes the whole dump,
     * then exits 1 because it could not read the binary log position, which a snapshot does not use
     * (CTAP-2267). This is the stderr mydumper 1.0.5 wrote for such a user against MySQL 8.4.
     */
    public function testLetsThroughAnExportThatFailedOnlyOnTheSourcePosition(): void
    {
        $this->assertTrue($this->failedOnlyOnSourcePosition(new ShellCommandFailedException(
            'mydumper …',
            1,
            '',
            self::STDERR_WITHOUT_REPLICATION_CLIENT
        )));
    }

    public function testRecognizesTheSourcePositionDenialByItsNewerWording(): void
    {
        $stderr = str_replace('master position', 'source position', self::STDERR_WITHOUT_REPLICATION_CLIENT);

        $failure = new ShellCommandFailedException('mydumper …', 1, '', $stderr);

        $this->assertTrue($this->failedOnlyOnSourcePosition($failure));
    }

    /**
     * @return iterable<string, array{0: \Exception}>
     */
    public static function provideFailuresThatAreNotOnlyTheSourcePosition(): iterable
    {
        $stderr = self::STDERR_WITHOUT_REPLICATION_CLIENT;
        yield 'another exit status' => [new ShellCommandFailedException('mydumper …', 2, '', $stderr)];
        yield 'a CRITICAL too' => [new ShellCommandFailedException('mydumper …', 1, '', $stderr
            . "** (mydumper:736): CRITICAL **: 15:04:55.556: Could not write schema for `mydb`.`t`\n")];
        yield 'another MySQL error' => [new ShellCommandFailedException('mydumper …', 1, '', $stderr
            . "** (mydumper:736): WARNING **: 15:04:55.556: Failed to execute SHOW INDEX over `mydb` - ERROR 1142: "
            . "SELECT command denied\n")];
        yield 'a different 1227' => [new ShellCommandFailedException('mydumper …', 1, '', str_replace(
            "Couldn't get master position",
            'Failed to disable binlog for the thread',
            $stderr
        ))];
        yield 'no source position denial at all' => [new ShellCommandFailedException('mydumper …', 1, '', '')];
        yield 'not a shell command failure' => [new \RuntimeException('mydumper …')];
    }

    #[DataProvider('provideFailuresThatAreNotOnlyTheSourcePosition')]
    public function testFailsAnExportThatFailedOnAnythingElse(\Exception $failure): void
    {
        $this->assertFalse($this->failedOnlyOnSourcePosition($failure));
    }

    private function failedOnlyOnSourcePosition(\Exception $failure): bool
    {
        $method = new ReflectionMethod(ExportPlugin::class, 'failedOnlyOnSourcePosition');

        return $method->invoke($this->createExportPlugin(''), $failure);
    }

    /**
     * @return array<string, string>
     */
    private function getMyDumperExportCommands(): array
    {
        $method = new ReflectionMethod(ExportPlugin::class, 'getMyDumperExportCommands');

        return $method->invoke($this->createExportPlugin(''), 'mydb', []);
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
