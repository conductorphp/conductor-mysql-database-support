<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorMySqlSupport\Adapter\Mydumper\RestorePreflight;
use ConductorMySqlSupport\Adapter\Mydumper\TargetSchemaSupport;
use ConductorMySqlSupport\Exception;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use ReflectionMethod;
use Stringable;

class RestorePreflightTest extends TestCase
{
    /**
     * The session setup mydumper writes above every definition, dumping from a MariaDB 10.6 source.
     */
    private const SQL_FILE_HEADER = <<<SQL
        /*!40101 SET NAMES binary*/;
        /*!40014 SET FOREIGN_KEY_CHECKS=0*/;
        /*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION'*/;
        /*!40103 SET TIME_ZONE='+00:00' */;

        SQL;

    /**
     * MariaDB allows a literal DEFAULT on a TEXT column; MySQL 8 rejects it with ERROR 1101. This is
     * the definition that got 71 tables into a database before myloader died on it.
     */
    private const REJECTED_TABLE = <<<SQL
        CREATE TABLE `permissions_holder` (
          `id` int(11) NOT NULL,
          `permissions` text NOT NULL DEFAULT 'a:0:{}',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

        SQL;

    private const ACCEPTED_TABLE = <<<SQL
        CREATE TABLE `good_one` (
          `id` int(11) NOT NULL,
          `name` varchar(64) NOT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

        SQL;

    private string $dumpDir;

    public function setUp(): void
    {
        $this->dumpDir = sys_get_temp_dir() . '/restore-preflight-' . bin2hex(random_bytes(6));
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
     * Table definitions are the only thing worth rehearsing. The CREATE DATABASE, the views and the
     * triggers reference objects a scratch database does not have, so trying them there would fail
     * definitions that restore perfectly well.
     */
    public function testReadsOnlyTheTableDefinitionsOutOfTheDump(): void
    {
        $this->writeDumpFile('mydb.good_one-schema.sql', self::SQL_FILE_HEADER . self::ACCEPTED_TABLE);
        $this->writeDumpFile('mydb.permissions_holder-schema.sql', self::SQL_FILE_HEADER . self::REJECTED_TABLE);
        $this->writeDumpFile(
            'mydb-schema-create.sql',
            self::SQL_FILE_HEADER . "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `mydb`;\n"
        );
        $this->writeDumpFile('mydb-schema-triggers.sql', self::SQL_FILE_HEADER);
        $this->writeDumpFile('mydb.a_view-schema-view.sql', self::SQL_FILE_HEADER . "CREATE VIEW `a_view` AS ...;\n");
        $this->writeDumpFile('metadata', "[snapshot]\n");

        $schemas = $this->readTableSchemas();

        $this->assertSame(['good_one', 'permissions_holder'], array_column($schemas, 'table'));
    }

    public function testReadsEachDefinitionWithTheSessionSetupMyLoaderWillApplyToIt(): void
    {
        $this->writeDumpFile('mydb.good_one-schema.sql', self::SQL_FILE_HEADER . self::ACCEPTED_TABLE);

        $schemas = $this->readTableSchemas();

        $this->assertSame(
            [
                'SET NAMES binary',
                'SET FOREIGN_KEY_CHECKS=0',
                "SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION'",
                "SET TIME_ZONE='+00:00'",
            ],
            $schemas[0]['sessionStatements']
        );
        $this->assertSame(rtrim(self::ACCEPTED_TABLE), $schemas[0]['createStatement']);
    }

    /**
     * The definition is taken whole rather than split on semicolons. A semicolon inside a DEFAULT or a
     * COMMENT would otherwise cut a valid definition in half and get it reported as a syntax error —
     * refusing a restore that would have worked, which is worse than not checking at all.
     */
    public function testKeepsADefinitionWholeWhenAValueContainsASemicolon(): void
    {
        $this->writeDumpFile('mydb.notes-schema.sql', self::SQL_FILE_HEADER . <<<SQL
            CREATE TABLE `notes` (
              `id` int(11) NOT NULL,
              `body` varchar(64) NOT NULL DEFAULT 'a;b' COMMENT 'semi; colon',
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB;

            SQL);

        $schemas = $this->readTableSchemas();

        $this->assertStringContainsString("DEFAULT 'a;b' COMMENT 'semi; colon'", $schemas[0]['createStatement']);
        $this->assertStringEndsWith(') ENGINE=InnoDB;', $schemas[0]['createStatement']);
    }

    public function testRefusesTheRestoreNamingTheTableAndTheServersReason(): void
    {
        $this->writeDumpFile('mydb.good_one-schema.sql', self::SQL_FILE_HEADER . self::ACCEPTED_TABLE);
        $this->writeDumpFile('mydb.permissions_holder-schema.sql', self::SQL_FILE_HEADER . self::REJECTED_TABLE);

        try {
            $this->check(
                [
                    [
                        'table' => 'permissions_holder',
                        'file' => 'mydb.permissions_holder-schema.sql',
                        'reason' => "ERROR 1101: BLOB, TEXT, GEOMETRY or JSON column 'permissions' can't have a "
                            . 'default value',
                    ],
                ]
            );
            $this->fail('Expected the restore to be refused.');
        } catch (Exception\RuntimeException $e) {
            $this->assertStringContainsString('will not accept 1 of the 2 table definition(s)', $e->getMessage());
            $this->assertStringContainsString('`permissions_holder`', $e->getMessage());
            $this->assertStringContainsString(
                "ERROR 1101: BLOB, TEXT, GEOMETRY or JSON column 'permissions'",
                $e->getMessage()
            );
            $this->assertStringContainsString('stopped before it changed anything', $e->getMessage());
        }
    }

    public function testLetsARestoreTheTargetAcceptsThroughUntouched(): void
    {
        $this->expectNotToPerformAssertions();
        $this->writeDumpFile('mydb.good_one-schema.sql', self::SQL_FILE_HEADER . self::ACCEPTED_TABLE);

        $this->check();
    }

    /**
     * A dump with no table definitions is not a reason to open a connection. The tab-delimited and
     * hand-built dumps that reach here have nothing to rehearse.
     */
    public function testDoesNotAskTheServerWhenTheDumpHasNoTableDefinitions(): void
    {
        $this->expectNotToPerformAssertions();
        $this->writeDumpFile('metadata', "[snapshot]\n");

        $this->check();
    }

    /**
     * The restore drops each table as it reaches it, so a target that already holds data is about to
     * lose it. The deploy plan asks first; the standalone import said nothing at all until now.
     */
    public function testSaysWhatIsAboutToBeReplaced(): void
    {
        $logger = $this->createRecordingLogger();

        $this->check([], 71, $logger);

        $this->assertStringContainsString('already contains 71 table(s)', implode("\n", $logger->messages));
    }

    public function testSaysNothingWhenTheTargetIsEmpty(): void
    {
        $logger = $this->createRecordingLogger();

        $this->check([], 0, $logger);

        $this->assertStringNotContainsString('already contains', implode("\n", $logger->messages));
    }

    /**
     * @param array[] $rejections What the stand-in target server refuses
     */
    private function check(array $rejections = [], ?int $tableCount = 0, ?AbstractLogger $logger = null): void
    {
        $target = new class ($rejections, $tableCount) extends TargetSchemaSupport {
            /** @var array[] */
            private array $rejections;
            private ?int $tableCount;

            /** @param array[] $rejections */
            public function __construct(array $rejections, ?int $tableCount)
            {
                parent::__construct('username', 'password');
                $this->rejections = $rejections;
                $this->tableCount = $tableCount;
            }

            public function getRejectedSchemas(array $schemas): array
            {
                return $this->rejections;
            }

            public function countTables(string $database): ?int
            {
                return $this->tableCount;
            }
        };

        (new RestorePreflight($target, $logger))->check($this->dumpDir, 'mydb');
    }

    /**
     * @return array[]
     */
    private function readTableSchemas(): array
    {
        $method = new ReflectionMethod(RestorePreflight::class, 'readTableSchemas');

        return $method->invoke(new RestorePreflight(new TargetSchemaSupport('username', 'password')), $this->dumpDir);
    }

    private function createRecordingLogger(): AbstractLogger
    {
        return new class extends AbstractLogger {
            /** @var string[] */
            public array $messages = [];

            public function log($level, Stringable|string $message, array $context = []): void
            {
                $this->messages[] = (string)$message;
            }
        };
    }

    private function writeDumpFile(string $name, string $contents): void
    {
        file_put_contents($this->dumpDir . '/' . $name, $contents);
    }
}
