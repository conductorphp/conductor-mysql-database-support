<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use PDO;
use PDOException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Answers "which of these sql_mode values will the target server refuse?" by asking the server itself.
 *
 * There is no list of valid modes to SELECT, so the only honest question is whether a SET is accepted.
 * A mode a server does not know is rejected with ERROR 1231, which is what we are looking for.
 */
class TargetSqlModeSupport
{
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
     * @param string[] $modes
     * @return string[] The subset of $modes the target server rejects, in the order given
     */
    public function getUnsupportedModes(array $modes): array
    {
        if (!$modes) {
            return [];
        }

        try {
            $connection = $this->connect();
        } catch (PDOException $e) {
            // Not fatal here. myloader is about to connect with the same credentials and will report
            // the connection problem far better than we can.
            $this->logger->warning(
                'Could not connect to the target server to check sql_mode support, so the dump is being '
                . 'loaded unchanged: ' . $this->host . ':' . $this->port . ' - ' . $e->getMessage()
            );
            return [];
        }

        // Fast path: if the target accepts every mode in the dump at once, there is nothing to strip and
        // the dump is left untouched. This is the MariaDB -> MariaDB case.
        if ($this->accepts($connection, implode(',', $modes))) {
            return [];
        }

        $unsupported = [];
        foreach ($modes as $mode) {
            if (!$this->accepts($connection, $mode)) {
                $unsupported[] = $mode;
            }
        }

        return $unsupported;
    }

    private function accepts(PDO $connection, string $sqlMode): bool
    {
        try {
            $statement = $connection->prepare('SET SESSION sql_mode = :sqlMode');
            $statement->execute([':sqlMode' => $sqlMode]);
            return true;
        } catch (PDOException $e) {
            return false;
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
