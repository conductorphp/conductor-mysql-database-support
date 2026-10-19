<?php

namespace ConductorMySqlSupport\Adapter;

use ConductorCore\Runtime\Requirement;
use ConductorCore\Runtime\RuntimeRequirementsInterface;
use ConductorCore\Database\DatabaseAdapterInterface;
use ConductorCore\Database\DatabaseEmptyingInterface;
use ConductorMySqlSupport\Exception;
use PDO;
use PDOException;
use PDOStatement;

class DatabaseAdapter implements DatabaseAdapterInterface, DatabaseEmptyingInterface, RuntimeRequirementsInterface
{
    private string $username;
    private string $password;
    private string $host;
    private string $port;
    private PDO $databaseConnection;
    private TlsOptions $tls;

    public function __construct(
        string $username,
        string $password,
        string $host = 'localhost',
        int    $port = 3306,
        ?TlsOptions $tls = null
    )
    {
        $this->tls = $tls ?? TlsOptions::disabled();
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
    }

    /**
     * @throws Exception\RuntimeException If error dropping db or db does not exist
     */
    /**
     * The driver behind the PDO connection. `ext-pdo_mysql` is suggested, not required, so a host
     * that never selects this adapter does not need it (CTAP-1759).
     *
     * @return list<Requirement>
     */
    public function runtimeRequirements(): array
    {
        return [Requirement::extension('pdo_mysql', 'docker-php-ext-install pdo_mysql')];
    }

    public function dropDatabaseIfExists(string $database): void
    {
        $statement = $this->runQuery("DROP DATABASE IF EXISTS " . $this->quoteIdentifier($database));
        if (0 === $statement->rowCount()) {
            throw new Exception\RuntimeException('Error dropping database "' . $database . '".');
        }
    }

    private function runQuery(string $query, ?array $data = null): PDOStatement
    {
        $this->connect();
        $statement = $this->databaseConnection->prepare($query);
        $statement->execute($data);

        return $statement;
    }

    private function connect(): void
    {
        if (!isset($this->databaseConnection)) {
            $this->databaseConnection = new PDO(
                "mysql:host={$this->host};port={$this->port};charset=UTF8;",
                $this->username,
                $this->password,
                $this->tls->pdoOptions()
            );
        }
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '`' . preg_replace('/[^A-Za-z0-9_]+/', '', $identifier) . '`';
    }

    /**
     * For a name read back from the server, which may hold any character: unlike quoteIdentifier(), it
     * escapes the name instead of stripping it
     */
    private function quoteObjectName(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    public function getDatabases(): array
    {
        return $this->runQuery("SHOW DATABASES")->fetchAll(PDO::FETCH_COLUMN);
    }

    public function getDatabaseMetadata(): array
    {
        $sql
            = "SELECT table_schema AS \"database\", 
                SUM(data_length + index_length) AS \"size\" 
                FROM information_schema.TABLES 
                GROUP BY table_schema
                ORDER BY table_schema";
        $statement = $this->runQuery($sql);
        $databases = [];
        foreach ($statement->fetchAll() as $row) {
            $databases[$row['database']] = [
                'size' => $row['size'],
            ];
        }
        return $databases;
    }

    public function getTableMetadata(string $database): array
    {
        // Every column is aliased because an unaliased one comes back under its REAL name, not the
        // case it was typed in, and information_schema.TABLES declares these columns uppercase.
        // Selecting `table_rows` and reading $row['table_rows'] therefore worked on MariaDB but not
        // on MySQL 8, which returns TABLE_ROWS: an "Undefined array key" warning per table, and a
        // null row count that database:table-metadata rendered as 0 for every table. Aliasing keeps
        // the read independent of server-side casing.
        $sql
            = "SELECT TABLE_NAME AS 'name', TABLE_ROWS AS 'rows', (data_length + index_length) 'size'
                FROM information_schema.TABLES
                WHERE table_schema = :database and TABLE_TYPE='BASE TABLE'
                ORDER BY TABLE_NAME ASC;";
        $statement = $this->runQuery($sql, [':database' => $database]);
        $tableSizes = [];
        foreach ($statement->fetchAll() as $row) {
            $tableSizes[$row['name']] = [
                'rows' => $row['rows'],
                'size' => $row['size'],
            ];
        }

        return $tableSizes;
    }

    public function databaseExists(string $database): bool
    {
        $sql = 'SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :database';
        $statement = $this->runQuery($sql, [':database' => $database]);
        return (bool)$statement->fetchColumn();
    }

    public function databaseIsEmpty(string $database): bool
    {
        $sql = 'SELECT COUNT(DISTINCT `table_name`) FROM `information_schema`.`columns` WHERE `table_schema` = :database';
        $statement = $this->runQuery($sql, [':database' => $database]);
        $numTables = (int)$statement->fetchColumn();
        return $numTables === 0;
    }

    public function dropDatabase(string $database): void
    {
        $this->runQuery("DROP DATABASE " . $this->quoteIdentifier($database));
    }

    public function createDatabase(string $database): void
    {
        $this->runQuery("CREATE DATABASE " . $this->quoteIdentifier($database));
    }

    /**
     * Needs only rights inside the database: DROP on its tables and views, ALTER ROUTINE on its
     * routines and EVENT for its events. Foreign key checks are off for the session while the tables go,
     * so they drop in any order. Triggers go with their tables. On MySQL 8, a view, routine or event
     * whose definer has SYSTEM_USER (root, say) also needs SYSTEM_USER to drop, so those go first: a
     * refusal stops the restore before any table is dropped.
     *
     * @throws Exception\RuntimeException If an object cannot be dropped.
     */
    public function emptyDatabase(string $database): void
    {
        $schema = $this->quoteIdentifier($database);
        $parameters = [':database' => trim($schema, '`')];
        $statements = $tableStatements = [];
        // TABLE_TYPE is VIEW or BASE TABLE, or on MariaDB also SYSTEM VERSIONED or SEQUENCE, which DROP
        // TABLE drops as well
        $tables = $this->runQuery(
            'SELECT TABLE_NAME AS name, TABLE_TYPE AS type FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = :database ORDER BY TABLE_NAME',
            $parameters
        )->fetchAll(PDO::FETCH_ASSOC);
        foreach ($tables as $table) {
            if ('VIEW' === $table['type']) {
                $statements[] = "DROP VIEW $schema." . $this->quoteObjectName($table['name']);
            } else {
                $tableStatements[] = "DROP TABLE $schema." . $this->quoteObjectName($table['name']);
            }
        }

        $routines = $this->runQuery(
            'SELECT ROUTINE_NAME AS name, ROUTINE_TYPE AS type FROM information_schema.ROUTINES
                WHERE ROUTINE_SCHEMA = :database AND ROUTINE_TYPE IN (\'PROCEDURE\', \'FUNCTION\')
                ORDER BY ROUTINE_TYPE, ROUTINE_NAME',
            $parameters
        )->fetchAll(PDO::FETCH_ASSOC);
        foreach ($routines as $routine) {
            $statements[] = "DROP {$routine['type']} $schema." . $this->quoteObjectName($routine['name']);
        }

        $events = $this->runQuery(
            'SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = :database ORDER BY EVENT_NAME',
            $parameters
        )->fetchAll(PDO::FETCH_COLUMN);
        foreach ($events as $event) {
            $statements[] = "DROP EVENT $schema." . $this->quoteObjectName($event);
        }
        $statements = [...$statements, ...$tableStatements];

        $this->runQuery('SET FOREIGN_KEY_CHECKS = 0');
        try {
            foreach ($statements as $statement) {
                try {
                    $this->runQuery($statement);
                } catch (PDOException $exception) {
                    throw new Exception\RuntimeException(
                        "Error emptying database \"$database\" ($statement): {$exception->getMessage()}",
                        (int)$exception->getCode(),
                        $exception
                    );
                }
            }
        } finally {
            $this->runQuery('SET FOREIGN_KEY_CHECKS = 1');
        }
    }

    /**
     * @param array<string, mixed>|null $parameters
     * @return list<array<string, mixed>>
     * @throws Exception\RuntimeException If the query cannot be run.
     */
    public function fetchAll(string $sql, string $database, ?array $parameters = null): array
    {
        // PDOException is caught and rethrown so a caller programming against
        // DatabaseAdapterInterface never has to catch a driver-specific exception to handle a
        // failed read. dropDatabaseIfExists() sets the same precedent.
        try {
            $this->runQuery("USE " . $this->quoteIdentifier($database));

            return $this->runQuery($sql, $parameters)->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $exception) {
            throw new Exception\RuntimeException(
                "Error running query against database \"$database\": {$exception->getMessage()}",
                (int)$exception->getCode(),
                $exception
            );
        }
    }

    public function quote(string $value): string
    {
        $this->connect();

        return $this->databaseConnection->quote($value);
    }

    /**
     * PDO does not have a clean way to run sql from a file or to run multiple SQL lines. We have to parse the SQL.
     */
    public function run(string $sql, string $database): void
    {
        $database = str_replace('`', '', $database);
        $this->runQuery("USE `$database`");

        // Remove single-line comments (starting with --)
        $sql = preg_replace('/--.*$/m', '', $sql); // Matches -- to end of line

        // Remove multi-line comments (starting with /* and ending with */)
        $sql = preg_replace('/\/\*.*?\*\//s', '', $sql); // Non-greedy match for /*...*/

        // Escape the semicolons inside the quoted strings by replacing them temporarily
        /** @var string $sql */
        $sql = preg_replace_callback('/(["\'])((?:\\1|.)*?)\1/', static function($matches) {
            return str_replace(';', '__SEMICOLON__', $matches[0]);
        }, $sql);

        // Trim any leading/trailing whitespace
        $sql = trim($sql);

        // Split queries based on semicolon (SQL statements should end with a semicolon)
        $queries = array_filter(array_map('trim', explode(';', $sql)));

        // Now, restore semicolons inside the strings
        $queries = array_map(static function($query) {
            return str_replace('__SEMICOLON__', ';', $query);
        }, $queries);

        foreach ($queries as $query) {
            $this->runQuery($query);
        }
    }
}
