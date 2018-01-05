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
        $username,
        $password,
        $host = 'localhost',
        $port = 3306
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
    public function dropDatabaseIfExists($name)
    {
        $result = $this->databaseConnection->exec("DROP DATABASE IF EXISTS asd" . $this->quoteIdentifier($name));
        if (!$result) {
            throw new Exception\RuntimeException('Error dropping database "' . $name . '".');
        }
    }

    /**
     *
     * @return string[]
     */
    public function getDatabases()
    {
        return $this->databaseConnection->query("SHOW DATABASES")->fetchColumn();
    }

    /**
     * @inheritdoc
     */
    public function getDatabaseMetadata()
    {
        $sql
            = "SELECT table_schema AS \"database\", 
                SUM(data_length + index_length) AS \"size\" 
                FROM information_schema.TABLES 
                GROUP BY table_schema
                ORDER BY table_schema";
        $query = $this->databaseConnection->prepare($sql);
        $query->execute();
        $databases = [];
        foreach ($query->fetchAll() as $row) {
            $databases[$row['database']] = [
                'size' => $row['size']
            ];
        }
        return $databases;
    }

    /**
     * @inheritdoc
     */
    public function getTableMetadata($database)
    {
        $sql
            = "SELECT TABLE_NAME, table_rows, (data_length + index_length) 'size'
                FROM information_schema.TABLES
                WHERE table_schema = :database and TABLE_TYPE='BASE TABLE'
                ORDER BY TABLE_NAME ASC;";
        $query = $this->databaseConnection->prepare($sql);
        $query->execute(
            [
                ':database' => $database,
            ]
        );
        $tableSizes = [];
        foreach ($query->fetchAll() as $row) {
            $tableSizes[$row['TABLE_NAME']] = [
                'rows' => $row['table_rows'],
                'size' => $row['size']
            ];
        }

        return $tableSizes;
    }

    private function quoteIdentifier($identifier)
    {
        return '`' . preg_replace('/[^A-Za-z0-9_]+/', '', $identifier) . '`';
    }
}
