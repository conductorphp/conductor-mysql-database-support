<?php

namespace ConductorMySqlSupport\Adapter;

use ConductorMySqlSupport\Exception;
use ConductorCore\Database\DatabaseAdapterInterface;
use PDO;
use PDOStatement;

class DatabaseAdapter implements DatabaseAdapterInterface
{
    /**
     * @var string
     */
    private $username;
    /**
     * @var string
     */
    private $password;
    /**
     * @var string
     */
    private $host;
    /**
     * @var string
     */
    private $port;

    /**
     * @var PDO
     */
    private $databaseConnection;

    public function __construct(
        string $username,
        string $password,
        string $host = 'localhost',
        int $port = 3306
    ) {
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
    }

    /**
     * @param string $database
     *
     * @throws Exception\RuntimeException If error dropping db or db does not exist
     */
    public function dropDatabaseIfExists(string $database): void
    {
        $statement = $this->runQuery("DROP DATABASE IF EXISTS " . $this->quoteIdentifier($database));
        if (0 === $statement->rowCount()) {
            throw new Exception\RuntimeException('Error dropping database "' . $database . '".');
        }
    }

    /**
     *
     * @return string[]
     */
    public function getDatabases(): array
    {
        $statement = $this->runQuery("SHOW DATABASES");
        return $statement->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * @return array
     */
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
                'size' => $row['size']
            ];
        }
        return $databases;
    }

    /**
     * @param string $database
     *
     * @return array
     */
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
                'size' => $row['size']
            ];
        }

        return $tableSizes;
    }

    /**
     * @param string $identifier
     *
     * @return string
     */
    private function quoteIdentifierSegment(string $identifier): string
    {
        return ('`' . trim(str_replace('`', '', $identifier)) . '`');
    }

    /**
     * @param string $identifier
     *
     * @return string
     */
    private function quoteIdentifier(string $identifier): string
    {
        {
            if (is_string($identifier)) {
                $identifier = explode('.', $identifier);
            }
            if (is_array($identifier)) {
                $segments = [];
                foreach ($identifier as $segment) {
                    $segments[] = $this->quoteIdentifierSegment($segment);

                }
                $quoted = implode('.', $segments);
            } else {
                $quoted = $this->quoteIdentifierSegment($identifier);
            }
            return $quoted;
        }
    }

    /**
     * @param string $database
     *
     * @return bool
     */
    public function databaseExists(string $database): bool
    {
        $sql = 'SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :database';
        $statement = $this->runQuery($sql, [':database' => $database]);
        return (bool) $statement->fetchColumn();
    }

    /**
     * @param string $database
     *
     * @return bool
     */
    public function databaseIsEmpty(string $database): bool
    {
        $sql = 'SELECT COUNT(DISTINCT `table_name`) FROM `information_schema`.`columns` WHERE `table_schema` = :database';
        $statement = $this->runQuery($sql, [':database' => $database]);
        $numTables = (int) $statement->fetchColumn();
        return $numTables == 0;
    }

    /**
     * @param string $database
     *
     * @return void
     */
    public function dropDatabase(string $database): void
    {
        $this->runQuery("DROP DATABASE " . $this->quoteIdentifier($database));
    }

    /**
     * @param string $database
     *
     * @return void
     */
    public function createDatabase(string $database): void
    {
        $this->runQuery("CREATE DATABASE " . $this->quoteIdentifier($database));
    }

    /**
     * @param string $query
     * @param string $database
     */
    public function run(string $query, string $database): void
    {
        $database = str_replace('`', '', $database);
        $this->runQuery("USE `$database`");
        $this->runQuery($query);
    }

    /**
     * @param string $query
     * @param array|null $data
     *
     * @return PDOStatement
     * @throws Exception\RuntimeException on error
     */
    private function runQuery(string $query, array $data = null): PDOStatement
    {
        $this->connect();
        $statement = $this->databaseConnection->prepare($query);
        if (false === $statement->execute($data)) {
            $message = sprintf("Error running query: %s\nError Code: %s\nError: %s",
                $statement->queryString,
                $statement->errorCode(),
                $statement->errorInfo()[2]
            );

            throw new Exception\RuntimeException($message);
        }

        return $statement;
    }

    private function connect(): void
    {
        if (is_null($this->databaseConnection)) {
            $this->databaseConnection = new PDO(
                "mysql:host={$this->host};port={$this->port};charset=UTF8;",
                $this->username,
                $this->password
            );
        }
    }
}
