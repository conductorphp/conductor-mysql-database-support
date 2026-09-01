<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use PDO;
use PDOException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Answers two questions about the target server that have to be asked before myloader runs.
 *
 * "Will this server accept these table definitions?" has no read-only form — MySQL has no dry run for
 * DDL — so the only honest way to ask is to create the tables somewhere harmless and see. That is a
 * scratch database, created and dropped by this class, which the real target never touches.
 *
 * "What is already in the target?" is a plain count, used to say what a restore is about to replace.
 */
class TargetSchemaSupport
{
    /**
     * Named so an operator who finds one after a killed process knows what left it and that it is
     * safe to drop. The random suffix keeps concurrent restores off each other's scratch space.
     */
    private const SCRATCH_DATABASE_PREFIX = 'conductor_restore_preflight_';

    private string $username;
    private string $password;
    private string $host;
    private int $port;
    private LoggerInterface $logger;
    private ?PDO $connection;

    public function __construct(
        string           $username,
        string           $password,
        string           $host = 'localhost',
        int              $port = 3306,
        ?LoggerInterface $logger = null,
        ?PDO             $connection = null
    ) {
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
        $this->logger = $logger ?? new NullLogger();
        $this->connection = $connection;
    }

    /**
     * @return int|null Tables currently in $database, or null if the server would not say
     */
    public function countTables(string $database): ?int
    {
        try {
            $connection = $this->connect();
            $statement = $connection->prepare(
                'SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = :database'
            );
            $statement->execute([':database' => $database]);

            return (int)$statement->fetchColumn();
        } catch (PDOException $e) {
            // Not fatal. myloader is about to connect with the same credentials and will report a
            // connection problem far better than we can; this only costs the operator a warning.
            $this->logger->warning(
                'Could not count the tables already in "' . $database . '" before restoring over them: '
                . $e->getMessage()
            );
            return null;
        }
    }

    /**
     * Creates every table definition in a scratch database and reports the ones the server refused.
     *
     * The scratch database is made with a bare CREATE DATABASE, which is exactly how conductor makes
     * the real one, so a table that inherits the database's default charset inherits the same one
     * here. Restoring into a database somebody else created with a different default is the one case
     * this does not model.
     *
     * @param array[] $schemas As returned by RestorePreflight::readTableSchemas()
     * @return array[] One ['table' => string, 'file' => string, 'reason' => string] per rejection
     */
    public function getRejectedSchemas(array $schemas): array
    {
        if (!$schemas) {
            return [];
        }

        try {
            $connection = $this->connect();
        } catch (PDOException $e) {
            $this->logger->warning(
                'Could not connect to the target server to check the dump\'s schema before restoring it, so the '
                . 'restore is starting unchecked: ' . $this->host . ':' . $this->port . ' - ' . $e->getMessage()
            );
            return [];
        }

        $scratchDatabase = self::SCRATCH_DATABASE_PREFIX . bin2hex(random_bytes(6));
        try {
            $connection->exec('CREATE DATABASE `' . $scratchDatabase . '`');
        } catch (PDOException $e) {
            // Checking costs a database this user may not be allowed to create. Say so rather than
            // failing the restore over a missing privilege — the restore itself may still work.
            $this->logger->warning(
                'Could not create a scratch database to check the dump\'s schema against this server, so the '
                . 'restore is starting unchecked: ' . $e->getMessage()
            );
            return [];
        }

        try {
            return $this->createSchemasIn($connection, $scratchDatabase, $schemas);
        } catch (PDOException $e) {
            // A definition the server refuses is caught per statement below, so reaching here means
            // the check itself broke — a dropped connection, a revoked grant. That is not evidence
            // against the dump, and refusing the restore over it would be its own kind of wrong.
            $this->logger->warning(
                'The check of the dump\'s schema could not be completed, so the restore is starting '
                . 'unchecked: ' . $e->getMessage()
            );
            return [];
        } finally {
            $this->dropScratchDatabase($connection, $scratchDatabase);
        }
    }

    /**
     * @param array[] $schemas
     * @return array[]
     */
    private function createSchemasIn(PDO $connection, string $scratchDatabase, array $schemas): array
    {
        $connection->exec('USE `' . $scratchDatabase . '`');
        // The dump sets this per file too, but the tables arrive in glob order, so a foreign key to a
        // table that has not been created yet has to be allowed here as well. myloader does the same.
        $connection->exec('SET SESSION FOREIGN_KEY_CHECKS = 0');

        $rejected = [];
        foreach ($schemas as $schema) {
            // The dump's own session setup — SET NAMES, TIME_ZONE, SQL_MODE — replayed the way
            // myloader replays it, so the definition is judged under the settings it will really be
            // created under. Anything the server rejects here is reported against the CREATE, not
            // against the setup: SqlModeSanitizer has already removed the modes this server refuses.
            foreach ($schema['sessionStatements'] as $sessionStatement) {
                try {
                    $connection->exec($sessionStatement);
                } catch (PDOException $e) {
                    $this->logger->debug(
                        'Target server refused "' . $sessionStatement . '" from ' . $schema['file'] . ': '
                        . $e->getMessage()
                    );
                }
            }

            try {
                $connection->exec($schema['createStatement']);
            } catch (PDOException $e) {
                $rejected[] = [
                    'table' => $schema['table'],
                    'file' => $schema['file'],
                    'reason' => $e->getMessage(),
                ];
            }
        }

        return $rejected;
    }

    private function dropScratchDatabase(PDO $connection, string $scratchDatabase): void
    {
        try {
            $connection->exec('DROP DATABASE `' . $scratchDatabase . '`');
        } catch (PDOException $e) {
            $this->logger->warning(
                'Failed to drop the scratch database "' . $scratchDatabase . '" used to check the dump\'s schema. '
                . 'It holds no data and can be dropped by hand: ' . $e->getMessage()
            );
        }
    }

    private function connect(): PDO
    {
        if (!isset($this->connection)) {
            $this->connection = new PDO(
                "mysql:host={$this->host};port={$this->port};charset=UTF8;",
                $this->username,
                $this->password,
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
            );
        }

        return $this->connection;
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }
}
