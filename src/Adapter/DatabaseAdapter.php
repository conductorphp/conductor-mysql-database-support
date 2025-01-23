<?php

namespace ConductorMySqlSupport\Adapter;

use ConductorCore\Database\DatabaseAdapterInterface;
use ConductorMySqlSupport\Exception;
use PDO;
use PDOStatement;

class DatabaseAdapter implements DatabaseAdapterInterface
{
    private string $username;
    private string $password;
    private string $host;
    private string $port;
    private PDO $databaseConnection;

    public function __construct(
        string $username,
        string $password,
        string $host = 'localhost',
        int    $port = 3306
    )
    {
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
    }

    /**
     * @throws Exception\RuntimeException If error dropping db or db does not exist
     */
    public function dropDatabaseIfExists(string $database): void
    {
        $statement = $this->runQuery("DROP DATABASE IF EXISTS " . $this->quoteIdentifier($database));
        if (0 === $statement->rowCount()) {
            throw new Exception\RuntimeException('Error dropping database "' . $database . '".');
        }
    }

    private function runQuery(string $query, array $data = null): PDOStatement
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
                $this->password
            );
        }
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '`' . preg_replace('/[^A-Za-z0-9_]+/', '', $identifier) . '`';
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
        $sql
            = "SELECT TABLE_NAME, table_rows, (data_length + index_length) 'size'
                FROM information_schema.TABLES
                WHERE table_schema = :database and TABLE_TYPE='BASE TABLE'
                ORDER BY TABLE_NAME ASC;";
        $statement = $this->runQuery($sql, [':database' => $database]);
        $tableSizes = [];
        foreach ($statement->fetchAll() as $row) {
            $tableSizes[$row['TABLE_NAME']] = [
                'rows' => $row['table_rows'],
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
