<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorCore\Shell\Adapter\LocalShellAdapter;
use ConductorMySqlSupport\Adapter\Mydumper\ImportPlugin;
use ConductorMySqlSupport\Adapter\Mydumper\RestorePreflight;
use ConductorMySqlSupport\Adapter\Mydumper\SqlModeSanitizer;
use ConductorMySqlSupport\Adapter\Mydumper\TargetSchemaSupport;
use ConductorMySqlSupport\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

/**
 * A snapshot restore asks the plugin whether the dump will load before it drops the database
 * (CTAP-2732). The answer must come from the dump's schema alone and leave the file where it was.
 */
class ImportPluginPreflightTest extends TestCase
{
    private string $dir;
    private string $dumpFile;
    /** @var array[] Table definitions the stand-in target server was asked to create */
    private array $checkedSchemas = [];

    public function setUp(): void
    {
        $this->dir = sys_get_temp_dir() . '/mydumper-preflight-' . bin2hex(random_bytes(6));
        mkdir("$this->dir/export-20261018", 0700, true);
        file_put_contents("$this->dir/export-20261018/app-schema-create.sql", "CREATE DATABASE `app`;\n");
        file_put_contents(
            "$this->dir/export-20261018/app.product-schema.sql",
            "/*!40101 SET NAMES binary*/;\nCREATE TABLE `product` (\n  `id` int NOT NULL\n);\n"
        );
        file_put_contents("$this->dir/export-20261018/app.product.00000.sql", "INSERT INTO `product` VALUES (1);\n");
        file_put_contents("$this->dir/export-20261018/metadata", "[config]\n");
        exec('cd ' . escapeshellarg($this->dir) . ' && tar czf app.tgz export-20261018 && rm -rf export-20261018');
        $this->dumpFile = "$this->dir/app.tgz";
        $this->checkedSchemas = [];
    }

    public function tearDown(): void
    {
        exec('rm -rf ' . escapeshellarg($this->dir));
    }

    public function testChecksTheDumpsTableDefinitions(): void
    {
        $this->plugin()->assertCanImportFile($this->dumpFile, 'app');

        $this->assertSame(['product'], array_column($this->checkedSchemas, 'table'));
    }

    public function testARefusedDumpIsReportedWithTheServersReason(): void
    {
        $plugin = $this->plugin([[
            'table' => 'product',
            'file' => 'app.product-schema.sql',
            'reason' => 'Specified key was too long; max key length is 3072 bytes',
        ]]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('max key length is 3072 bytes');
        $plugin->assertCanImportFile($this->dumpFile, 'app');
    }

    /**
     * importFromFile() extracts the same file afterwards, and deletes it once it has.
     */
    public function testLeavesTheDumpFileAndNoScratchDirectory(): void
    {
        $before = $this->scratchDirectories();

        try {
            $this->plugin([['table' => 'product', 'file' => 'app.product-schema.sql', 'reason' => 'no']])
                ->assertCanImportFile($this->dumpFile, 'app');
        } catch (RuntimeException) {
        }
        $this->plugin()->assertCanImportFile($this->dumpFile, 'app');

        $this->assertFileExists($this->dumpFile);
        $this->assertSame(['app.tgz'], array_values(array_diff(scandir($this->dir), ['.', '..'])));
        $this->assertSame($before, $this->scratchDirectories());
    }

    public function testRefusesAFileThatIsNotAMydumperExport(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Should be .tgz');
        $this->plugin()->assertCanImportFile("$this->dir/app.sql", 'app');
    }

    /**
     * @param array[] $rejections What the stand-in target server refuses
     */
    private function plugin(array $rejections = []): ImportPlugin
    {
        $test = $this;
        $target = new class ($rejections, $test) extends TargetSchemaSupport {
            /** @param array[] $rejections */
            public function __construct(private array $rejections, private ImportPluginPreflightTest $test)
            {
                parent::__construct('username', 'password');
            }

            public function getRejectedSchemas(array $schemas): array
            {
                $this->test->recordCheckedSchemas($schemas);
                return $this->rejections;
            }
        };

        // assertIsUsable() needs myloader on the PATH, which says nothing about the check itself
        return new class (
            new LocalShellAdapter(),
            'username',
            'password',
            'localhost',
            3306,
            null,
            $this->createStub(SqlModeSanitizer::class),
            new RestorePreflight($target)
        ) extends ImportPlugin {
            public function assertIsUsable(): void
            {
            }
        };
    }

    /**
     * @param array[] $schemas
     */
    public function recordCheckedSchemas(array $schemas): void
    {
        $this->checkedSchemas = $schemas;
    }

    /**
     * @return string[]
     */
    private function scratchDirectories(): array
    {
        return glob(sys_get_temp_dir() . '/conductor-restore-preflight-*') ?: [];
    }
}
