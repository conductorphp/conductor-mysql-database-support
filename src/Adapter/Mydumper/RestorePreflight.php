<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorMySqlSupport\Exception;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Decides whether a restore can succeed before myloader starts destroying the target.
 *
 * myloader drops and recreates each table as it reaches it, so a dump this server cannot load does
 * not fail cleanly — it fails partway, having already replaced whatever was there. A pre-fix snapshot
 * measured against a MySQL 8 target got 71 tables in before it died. Everything those 71 tables
 * replaced was gone, for a restore that was never going to finish.
 *
 * So the schema is tried first, in a scratch database, and the restore is refused with the server's
 * own reason if the target will not take it. Nothing about the real database changes on that path.
 *
 * Only table definitions are checked, which is where the incompatibilities live — a literal DEFAULT
 * on a TEXT column, an index too long for the target's charset, a collation the target does not know.
 * Views, triggers and routines are deliberately left out: they reference objects that do not exist
 * in a scratch database, so trying them there would fail definitions that restore perfectly well.
 */
class RestorePreflight
{
    private const MAX_REPORTED_REJECTIONS = 10;

    /**
     * mydumper writes one file per table as "<db>.<table>-schema.sql". Its other schema files end in
     * -schema-create.sql (the CREATE DATABASE), -schema-view.sql, -schema-triggers.sql and
     * -schema-post.sql, none of which this glob matches.
     */
    private const TABLE_SCHEMA_GLOB = '/*-schema.sql';

    /**
     * The version-gated session setup mydumper writes above each definition, e.g.
     * "/*!40101 SET NAMES binary*\/;". Group 1 is the statement without its gate.
     */
    private const SESSION_STATEMENT_PATTERN = '~^\s*/\*!\d+\s+(.+?)\s*\*/\s*;\s*$~';

    private TargetSchemaSupport $targetSchemaSupport;
    private LoggerInterface $logger;

    public function __construct(
        TargetSchemaSupport $targetSchemaSupport,
        ?LoggerInterface    $logger = null
    ) {
        $this->targetSchemaSupport = $targetSchemaSupport;
        $this->logger = $logger ?? new NullLogger();
    }

    /**
     * @param string $extractedPath Path to an extracted, sql_mode-sanitized mydumper dump directory
     * @param string $database Database the dump is about to be restored into
     * @throws Exception\RuntimeException If the target server will not accept the dump's schema
     */
    public function check(string $extractedPath, string $database): void
    {
        $this->assertSchemaLoads($extractedPath);
        $this->warnAboutWhatIsAboutToBeReplaced($database);
    }

    /**
     * @throws Exception\RuntimeException
     */
    private function assertSchemaLoads(string $extractedPath): void
    {
        $schemas = $this->readTableSchemas($extractedPath);
        if (!$schemas) {
            return;
        }

        $this->logger->info(
            sprintf('Checking the dump\'s %d table definition(s) against the target server.', count($schemas))
        );

        $rejections = $this->targetSchemaSupport->getRejectedSchemas($schemas);
        if (!$rejections) {
            return;
        }

        $reported = array_slice($rejections, 0, self::MAX_REPORTED_REJECTIONS);
        $reasons = [];
        foreach ($reported as $rejection) {
            $reasons[] = sprintf('`%s` (%s): %s', $rejection['table'], $rejection['file'], $rejection['reason']);
        }
        if (count($rejections) > count($reported)) {
            $reasons[] = sprintf('...and %d more.', count($rejections) - count($reported));
        }

        throw new Exception\RuntimeException(
            sprintf(
                "The target server will not accept %d of the %d table definition(s) in this dump:\n  %s\n"
                . 'The restore was stopped before it changed anything, because a restore that fails partway '
                . 'leaves the database half replaced. Either restore this dump onto a server that accepts it, '
                . 'or take a new snapshot from a source the target can load.',
                count($rejections),
                count($schemas),
                implode("\n  ", $reasons)
            )
        );
    }

    /**
     * A restore into a database that already has tables replaces them, and myloader does it table by
     * table with no way back. The deploy plan drops the database first and asks before it does; the
     * standalone import does not, and until now said nothing at all.
     */
    private function warnAboutWhatIsAboutToBeReplaced(string $database): void
    {
        $tableCount = $this->targetSchemaSupport->countTables($database);
        if (!$tableCount) {
            return;
        }

        $this->logger->warning(
            sprintf(
                'Database "%s" already contains %d table(s). This restore drops and recreates each table the '
                . 'dump contains, so their current contents are about to be replaced and cannot be recovered '
                . 'from here.',
                $database,
                $tableCount
            )
        );
    }

    /**
     * Reads each table definition out of the dump, along with the session setup mydumper wrote above
     * it. Everything from the CREATE line to the end of the file is taken as one statement rather
     * than split on semicolons: these files hold exactly one definition, and a semicolon inside a
     * DEFAULT or a COMMENT would cut a valid definition in half and report it as a syntax error.
     *
     * @return array[] One ['table' => string, 'file' => string, 'sessionStatements' => string[],
     *                 'createStatement' => string] per table definition found
     */
    private function readTableSchemas(string $extractedPath): array
    {
        $schemas = [];
        foreach (glob($extractedPath . self::TABLE_SCHEMA_GLOB) ?: [] as $file) {
            $lines = file($file, FILE_IGNORE_NEW_LINES);
            if (false === $lines) {
                $this->logger->warning("Could not read \"$file\" while checking the dump's schema.");
                continue;
            }

            $sessionStatements = [];
            $createLines = [];
            foreach ($lines as $line) {
                if (!$createLines) {
                    if (preg_match(self::SESSION_STATEMENT_PATTERN, $line, $matches)) {
                        $sessionStatements[] = $matches[1];
                        continue;
                    }
                    if (!preg_match('~^\s*CREATE\s~i', $line)) {
                        continue;
                    }
                }
                $createLines[] = $line;
            }

            $createStatement = rtrim(implode("\n", $createLines));
            if ('' === $createStatement) {
                continue;
            }

            $schemas[] = [
                'table' => $this->getTableName($createStatement, $file),
                'file' => basename($file),
                'sessionStatements' => $sessionStatements,
                'createStatement' => $createStatement,
            ];
        }

        return $schemas;
    }

    private function getTableName(string $createStatement, string $file): string
    {
        if (preg_match('~^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([^`\s(]+)~i', $createStatement, $matches)) {
            return $matches[1];
        }

        return basename($file, '-schema.sql');
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
        $this->targetSchemaSupport->setLogger($logger);
    }
}
