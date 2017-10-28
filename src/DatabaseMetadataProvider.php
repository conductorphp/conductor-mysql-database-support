<?php

namespace DevopsToolMySqlSupport;

use DevopsToolCore\Database\DatabaseMetadataProviderInterface;

class DatabaseMetadataProvider implements DatabaseMetadataProviderInterface
{
    /**
     * @var \PDO
     */
    private $connection;

    public function __construct(\PDO $connection)
    {
        $this->connection = $connection;
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
        $query = $this->connection->prepare($sql);
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
        $query = $this->connection->prepare($sql);
        $query->execute([
            ':database' => $database,
        ]);
        $tableSizes = [];
        foreach ($query->fetchAll() as $row) {
            $tableSizes[$row['TABLE_NAME']] = [
                'rows' => $row['table_rows'],
                'size' => $row['size']
            ];
        }

        return $tableSizes;
    }

}
