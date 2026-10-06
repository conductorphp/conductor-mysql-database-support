<?php

namespace ConductorMySqlSupportTest\Adapter;

use ConductorCore\Shell\Adapter\LocalShellAdapter;
use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Adapter\ClientCredentials;
use ConductorMySqlSupport\Adapter\Mydumper;
use ConductorMySqlSupport\Adapter\Mysqldump;
use ConductorMySqlSupport\Adapter\TabDelimited;
use ConductorMySqlSupport\Adapter\TlsOptions;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use ReflectionMethod;
use RuntimeException;
use Stringable;

/**
 * The password reaches the clients as MYSQL_PWD, never as an argument: a command line is what a
 * failed step reports, and that report is logged at ERROR (CTAP-2218).
 */
class ClientCredentialsTest extends TestCase
{
    private const PASSWORD = "pa'ss \"w0rd\"";

    private string $binDir;
    private string|false $path;

    public function setUp(): void
    {
        $this->binDir = sys_get_temp_dir() . '/mysql-credentials-' . bin2hex(random_bytes(6));
        mkdir($this->binDir);
        $this->path = getenv('PATH');
    }

    public function tearDown(): void
    {
        putenv(false === $this->path ? 'PATH' : 'PATH=' . $this->path);
        foreach (glob($this->binDir . '/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($this->binDir);
    }

    public function testConnectionArgumentsLeaveThePasswordOut(): void
    {
        $credentials = new ClientCredentials('app', self::PASSWORD, 'db.example', 3307);

        $this->assertSame("-h 'db.example' -P '3307' -u 'app'", $credentials->connectionArguments());
    }

    public function testTheEnvironmentCarriesThePasswordOnTopOfConductorsOwn(): void
    {
        $environment = (new ClientCredentials('app', self::PASSWORD, 'db', 3306))->environment();

        $this->assertSame(self::PASSWORD, $environment['MYSQL_PWD']);
        $this->assertSame(getenv('PATH'), $environment['PATH']);
    }

    /** Without a password the command inherits conductor's environment, as it always did. */
    public function testNoPasswordMeansNoEnvironmentOfItsOwn(): void
    {
        $this->assertNull((new ClientCredentials('app', '', 'db', 3306))->environment());
    }

    /** @return iterable<string, array{object, string}> */
    public static function plugins(): iterable
    {
        $shell = new class implements ShellAdapterInterface {
            public function isCallable(string $command): bool
            {
                return true;
            }

            public function runShellCommand(
                string $command,
                ?string $currentWorkingDirectory = null,
                ?array $environmentVariables = null,
                int $priority = self::PRIORITY_NORMAL,
                ?array $options = null
            ): string {
                return '';
            }
        };
        $tls = TlsOptions::fromOptions(['tls' => true, 'tls_verify' => false]);

        yield 'mydumper export, mydumper' => [
            new Mydumper\ExportPlugin($shell, 'app', self::PASSWORD, 'db', 3306, null, $tls),
            'getMysqldumperCommandConnectionArguments',
        ];
        yield 'mydumper export, mysql' => [
            new Mydumper\ExportPlugin($shell, 'app', self::PASSWORD, 'db', 3306, null, $tls),
            'getMysqlCommandConnectionArguments',
        ];
        yield 'mydumper import' => [
            new Mydumper\ImportPlugin($shell, 'app', self::PASSWORD, 'db', 3306, null, null, null, $tls),
            'getMysqlCommandConnectionArguments',
        ];
        yield 'mysqldump export' => [
            new Mysqldump\ExportPlugin($shell, 'app', self::PASSWORD, 'db', 3306, null, $tls),
            'getCommandConnectionArguments',
        ];
        yield 'mysqldump import' => [
            new Mysqldump\ImportPlugin($shell, 'app', self::PASSWORD, 'db', 3306, null, $tls),
            'getMysqlCommandConnectionArguments',
        ];
        yield 'tab-delimited export' => [
            new TabDelimited\ExportPlugin($shell, 'app', self::PASSWORD, 'db', 3306, null, $tls),
            'getMysqlCommandConnectionArguments',
        ];
        yield 'tab-delimited import' => [
            new TabDelimited\ImportPlugin($shell, 'app', self::PASSWORD, 'db', 3306, null, $tls),
            'getMysqlCommandConnectionArguments',
        ];
    }

    #[DataProvider('plugins')]
    public function testNoPluginPutsThePasswordOnTheCommandLine(object $plugin, string $method): void
    {
        $arguments = (string) (new ReflectionMethod($plugin, $method))->invoke($plugin);

        $this->assertStringStartsWith("-h 'db' -P '3306' -u 'app' --", $arguments);
        $this->assertStringNotContainsString(self::PASSWORD, $arguments);
        $this->assertDoesNotMatchRegularExpression('/(^|\s)(-p|--password)/', $arguments);
    }

    /**
     * End to end through the real shell adapter: a mysql that fails, as a wrong host or a dropped
     * connection would. The client still gets the password, and nothing reported carries it.
     */
    public function testAFailingImportAuthenticatesButReportsNoPassword(): void
    {
        $seen = $this->binDir . '/seen';
        $this->stubBinary('mysql', <<<SH
            #!/usr/bin/env bash
            printf 'argv=%s\\npwd=%s\\n' "\$*" "\${MYSQL_PWD:-}" > '$seen'
            echo "ERROR 2005 (HY000): Unknown server host 'db'" >&2
            exit 1
            SH);
        $this->stubBinary('gunzip', "#!/usr/bin/env bash\nexit 0\n");
        putenv('PATH=' . $this->binDir . ':' . $this->path);
        $dump = $this->binDir . '/dump.sql';
        file_put_contents($dump, "SELECT 1;\n");

        $logger = $this->recordingLogger();
        $plugin = new Mysqldump\ImportPlugin(new LocalShellAdapter($logger), 'app', self::PASSWORD, 'db', 3306, $logger);

        try {
            $plugin->importFromFile($dump, 'mydb');
            $this->fail('Expected the import to fail.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('An error occurred while running shell command', $exception->getMessage());
            $this->assertStringNotContainsString(self::PASSWORD, $exception->getMessage());
        }

        $this->assertSame(
            "argv=mydb -h db -P 3306 -u app\npwd=" . self::PASSWORD . "\n",
            file_get_contents($seen)
        );
        $this->assertNotEmpty($logger->messages);
        foreach ($logger->messages as $message) {
            $this->assertStringNotContainsString(self::PASSWORD, $message);
        }
    }

    private function stubBinary(string $name, string $script): void
    {
        file_put_contents("$this->binDir/$name", $script);
        chmod("$this->binDir/$name", 0700);
    }

    private function recordingLogger(): AbstractLogger
    {
        return new class extends AbstractLogger {
            public array $messages = [];

            public function log($level, string|Stringable $message, array $context = []): void
            {
                $this->messages[] = (string) $message;
            }
        };
    }
}
