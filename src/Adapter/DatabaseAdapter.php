<?php

namespace DevopsToolMySqlSupport\Adapter;

use DevopsToolMySqlSupport\Exception;
use DevopsToolCore\Database\DatabaseAdapterInterface;
use PDO;

class DatabaseAdapter implements DatabaseAdapterInterface
{
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
        $this->databaseConnection = new PDO(
            "mysql:host={$host};port={$port};charset=UTF8;",
            $username,
            $password
        );
    }

    /**
     * @param string $name
     * @throws Exception\RuntimeException If error dropping db
     */
    public function dropDatabaseIfExists(string $name): void
    {
        $result = $this->databaseConnection->exec("DROP DATABASE IF EXISTS " . $this->quoteIdentifier($name));
        if (false === $result) {
            throw new Exception\RuntimeException('Error dropping database "' . $name . '".');
        }
    }

    /**
     *
     * @return string[]
     */
    public function getDatabases(): array
    {
        return $this->databaseConnection->query("SHOW DATABASES")->fetchColumn();
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
        $statement = $this->databaseConnection->prepare($sql);
        $statement->execute();
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
        $statement = $this->databaseConnection->prepare($sql);
        $statement->execute(
            [
                ':database' => $database,
            ]
        );
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
    private function quoteIdentifier(string $identifier): string
    {
        return '`' . preg_replace('/[^A-Za-z0-9_]+/', '', $identifier) . '`';
    }

    /**
     * @param string $database
     *
     * @return bool
     */
    public function databaseExists(string $database): bool
    {
        $sql = 'SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :database';
        $statement = $this->databaseConnection->prepare($sql);
        $statement->execute([':database' => $database]);
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
        $statement = $this->databaseConnection->prepare($sql);
        $statement->execute([':database' => $database]);
        return (bool) $statement->fetchColumn();
    }

    /**
     * @param string $database
     *
     * @return void
     */
    public function dropDatabase(string $database): void
    {
        $database = str_replace('`', '', $database);
        $stmt = $this->databaseConnection->query("DROP DATABASE IF EXISTS `$database`");
        $stmt->execute();
    }

    /**
     * @param string $database
     *
     * @return void
     */
    public function createDatabase(string $database): void
    {
        $database = str_replace('`', '', $database);
        $stmt = $this->databaseConnection->query("CREATE DATABASE `$database`");
        $stmt->execute();
    }

    /**
     * @param string $query
     * @param string $database
     */
    public function run(string $query, string $database): void
    {
        $database = str_replace('`', '', $database);
        $stmt = $this->databaseConnection->query("USE `$database`; $query");
        $stmt->execute();
    }
}
