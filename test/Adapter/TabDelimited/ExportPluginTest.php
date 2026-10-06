<?php

namespace ConductorMySqlSupportTest\Adapter\TabDelimited;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Adapter\TabDelimited\ExportPlugin;
use ConductorMySqlSupport\Adapter\TlsOptions;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

class ExportPluginTest extends TestCase
{
    private const CONNECTION = "-h 'db.example' -P '3307' -u 'app'";

    /**
     * The per-table data dump had no connection arguments, so it read from the client's default
     * server instead of the configured one (CTAP-2221).
     */
    public function testEveryMysqlInvocationConnectsToTheConfiguredServer(): void
    {
        $command = $this->exportCommand(250000);

        $this->assertSame(3, substr_count($command, "mysql 'mydb' --skip-column-names -e \"SELECT * FROM"));
        preg_match_all('/(?<![\w-])(mysql|mysqldump) [^|&]*/', $command, $invocations);
        $this->assertNotEmpty($invocations[0]);
        foreach ($invocations[0] as $invocation) {
            $this->assertStringContainsString(self::CONNECTION, $invocation);
            $this->assertStringContainsString('--loose-ssl-mode=REQUIRED', $invocation);
        }
    }

    /** LIMIT is offset,count. Using the end offset as the count made each file overlap the next. */
    public function testSplitsATableIntoFilesThatDoNotOverlap(): void
    {
        $command = $this->exportCommand(250000);

        preg_match_all('/LIMIT (\d+),(\d+)"/', $command, $limits, PREG_SET_ORDER);
        $this->assertSame(
            [['0', '100000'], ['100000', '100000'], ['200000', '100000']],
            array_map(static fn (array $limit): array => [$limit[1], $limit[2]], $limits)
        );
    }

    private function exportCommand(int $rows): string
    {
        $shell = new class ($rows) implements ShellAdapterInterface {
            public function __construct(private int $rows)
            {
            }

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
                return match (true) {
                    str_contains($command, 'SHOW TABLES') => "product\n",
                    str_contains($command, 'COUNT(*)') => "$this->rows\n",
                    str_contains($command, 'COLUMN_KEY') => "id\n",
                    default => '',
                };
            }
        };
        $plugin = new ExportPlugin(
            $shell,
            'app',
            'secret',
            'db.example',
            3307,
            null,
            TlsOptions::fromOptions(['tls' => true, 'tls_verify' => false])
        );

        return (string) (new ReflectionMethod($plugin, 'getTabDelimitedFileExportCommand'))
            ->invoke($plugin, 'mydb', '/tmp/export', ['ignore_tables' => []]);
    }
}
