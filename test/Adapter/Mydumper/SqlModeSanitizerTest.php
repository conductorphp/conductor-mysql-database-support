<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorMySqlSupport\Adapter\Mydumper\SqlModeSanitizer;
use ConductorMySqlSupport\Adapter\Mydumper\TargetSqlModeSupport;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Stringable;

class SqlModeSanitizerTest extends TestCase
{
    /**
     * The header mydumper 0.19 writes at the top of every .sql file, dumping from a MariaDB 10.6 source.
     */
    private const SQL_FILE_HEADER = <<<SQL
        /*!40101 SET NAMES binary*/;
        /*!40014 SET FOREIGN_KEY_CHECKS=0*/;
        /*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
        /*!40103 SET TIME_ZONE='+00:00' */;

        SQL;

    /**
     * The metadata file from the same dump. myloader applies [myloader_session_variables] to every
     * connection it opens, which is why a mode the target rejects kills threads before any data is read.
     */
    private const METADATA = <<<METADATA
        # Started dump at: 2026-08-31 21:44:48
        [config]
        quote-character = BACKTICK

        [myloader_session_variables]
        SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' /*!40101


        [`middleware`.`product`]
        real_table_name=product
        rows = 2
        # Finished dump at: 2026-08-31 21:44:48

        METADATA;

    private string $dumpDir;

    public function setUp(): void
    {
        $this->dumpDir = sys_get_temp_dir() . '/sql-mode-sanitizer-' . bin2hex(random_bytes(6));
        mkdir($this->dumpDir);
    }

    public function tearDown(): void
    {
        foreach (glob($this->dumpDir . '/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($this->dumpDir);
    }

    public function testRemovesUnsupportedModeFromSqlFileHeader(): void
    {
        $this->writeDumpFile('middleware.product-schema.sql', self::SQL_FILE_HEADER . "CREATE TABLE `product`;\n");

        $this->sanitize(['NO_AUTO_CREATE_USER']);

        $this->assertStringContainsString(
            "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
            $this->readDumpFile('middleware.product-schema.sql')
        );
    }

    public function testRemovesUnsupportedModeFromMetadataSessionVariables(): void
    {
        $this->writeDumpFile('metadata', self::METADATA);

        $this->sanitize(['NO_AUTO_CREATE_USER']);

        $metadata = $this->readDumpFile('metadata');
        $this->assertStringNotContainsString('NO_AUTO_CREATE_USER', $metadata);
        $this->assertStringContainsString("SQL_MODE='NO_AUTO_VALUE_ON_ZERO,", $metadata);
        // The rest of the metadata has to survive — myloader reads its table groups out of it.
        $this->assertStringContainsString('[myloader_session_variables]', $metadata);
        $this->assertStringContainsString('[`middleware`.`product`]', $metadata);
        $this->assertStringContainsString('rows = 2', $metadata);
    }

    /**
     * The rewrite is padded back to the original byte length so only the header block has to be
     * written, rather than every file in a multi-gigabyte dump being rewritten end to end.
     */
    public function testRewriteLeavesFileLengthUnchanged(): void
    {
        $contents = self::SQL_FILE_HEADER . "INSERT INTO `product` VALUES(1,\"a\");\n";
        $this->writeDumpFile('middleware.product.00000.sql', $contents);

        $this->sanitize(['NO_AUTO_CREATE_USER']);

        $this->assertSame(strlen($contents), strlen($this->readDumpFile('middleware.product.00000.sql')));
    }

    public function testLeavesDataBelowTheHeaderUntouched(): void
    {
        $data = "INSERT INTO `product` VALUES(1,\"SET SQL_MODE='NO_AUTO_CREATE_USER'\");\n";
        $this->writeDumpFile('middleware.product.00000.sql', self::SQL_FILE_HEADER . $data);

        $this->sanitize(['NO_AUTO_CREATE_USER']);

        $this->assertStringContainsString($data, $this->readDumpFile('middleware.product.00000.sql'));
    }

    /**
     * The MariaDB -> MariaDB case: the target accepts every mode in the dump, so nothing is written.
     */
    public function testTargetAcceptingEveryModeLeavesTheDumpByteForByteIdentical(): void
    {
        $this->writeDumpFile('metadata', self::METADATA);
        $this->writeDumpFile('middleware.product-schema.sql', self::SQL_FILE_HEADER);
        $before = $this->checksumDump();

        $this->sanitize([]);

        $this->assertSame($before, $this->checksumDump());
    }

    public function testRemovingEveryModeLeavesAnEmptyButValidModeList(): void
    {
        $this->writeDumpFile('middleware.product-schema.sql', self::SQL_FILE_HEADER);

        $this->sanitize([
            'NO_AUTO_VALUE_ON_ZERO',
            'ERROR_FOR_DIVISION_BY_ZERO',
            'NO_AUTO_CREATE_USER',
            'NO_ENGINE_SUBSTITUTION',
        ]);

        $this->assertStringContainsString(
            "/*!40101 SET SQL_MODE=''",
            $this->readDumpFile('middleware.product-schema.sql')
        );
    }

    public function testLogsWhatItRemovedAndFromHowManyFiles(): void
    {
        $this->writeDumpFile('metadata', self::METADATA);
        $this->writeDumpFile('middleware.product-schema.sql', self::SQL_FILE_HEADER);
        $this->writeDumpFile('middleware.product.00000.sql', self::SQL_FILE_HEADER);
        $logger = $this->createRecordingLogger();

        $this->sanitize(['NO_AUTO_CREATE_USER'], $logger);

        $this->assertCount(1, $logger->messages);
        $this->assertStringContainsString('NO_AUTO_CREATE_USER', $logger->messages[0]);
        $this->assertStringContainsString('3 file(s)', $logger->messages[0]);
    }

    public function testSaysNothingWhenTheDumpNamesNoSqlMode(): void
    {
        $this->writeDumpFile('middleware.product-schema.sql', "/*!40101 SET NAMES binary*/;\n");
        $logger = $this->createRecordingLogger();

        $this->sanitize(['NO_AUTO_CREATE_USER'], $logger);

        $this->assertSame([], $logger->messages);
    }

    /**
     * @param string[] $unsupportedModes The modes the stand-in target server rejects
     */
    private function sanitize(array $unsupportedModes, ?AbstractLogger $logger = null): void
    {
        $target = new class ($unsupportedModes) extends TargetSqlModeSupport {
            /** @var string[] */
            private array $unsupportedModes;

            /** @param string[] $unsupportedModes */
            public function __construct(array $unsupportedModes)
            {
                parent::__construct('username', 'password');
                $this->unsupportedModes = $unsupportedModes;
            }

            public function getUnsupportedModes(array $modes): array
            {
                return array_values(array_intersect($modes, $this->unsupportedModes));
            }
        };

        (new SqlModeSanitizer($target, $logger))->sanitize($this->dumpDir);
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

    private function readDumpFile(string $name): string
    {
        return (string)file_get_contents($this->dumpDir . '/' . $name);
    }

    /**
     * @return array<string,string>
     */
    private function checksumDump(): array
    {
        $checksums = [];
        foreach (glob($this->dumpDir . '/*') ?: [] as $file) {
            $checksums[basename($file)] = md5_file($file);
        }

        return $checksums;
    }
}
