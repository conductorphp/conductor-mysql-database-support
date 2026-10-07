<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorMySqlSupport\Adapter\TlsOptions;
use PDO;
use PDOException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Answers "may this user keep its restore out of the binary log?" by asking the server itself.
 *
 * myloader opens every connection with SET SESSION SQL_LOG_BIN = 0 unless told --enable-binlog, and
 * that SET needs SUPER, SYSTEM_VARIABLES_ADMIN or SESSION_VARIABLES_ADMIN. A managed database's app
 * user has none of them (it holds ALL PRIVILEGES on its own database only), so myloader logs ERROR
 * 1227 on each connection, counts every one as an error and exits 1 however well the restore went.
 * There is no privilege to SELECT that settles it across MySQL, MariaDB and Aurora, so try the SET.
 */
class TargetBinlogSupport
{
    private string $username;
    private string $password;
    private string $host;
    private int $port;
    private LoggerInterface $logger;
    private ?PDO $connection;

    private TlsOptions $tls;

    public function __construct(
        string           $username,
        string           $password,
        string           $host = 'localhost',
        int              $port = 3306,
        ?LoggerInterface $logger = null,
        ?PDO             $connection = null,
        ?TlsOptions $tls = null
    ) {
        $this->tls = $tls ?? TlsOptions::disabled();
        $this->username = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
        $this->logger = $logger ?? new NullLogger();
        $this->connection = $connection;
    }

    public function canDisableBinlog(): bool
    {
        try {
            $connection = $this->connect();
        } catch (PDOException $e) {
            // Not this check's failure to report. myloader is about to connect with the same
            // credentials and will say why it cannot; until then, keep its default.
            $this->logger->warning(
                'Could not connect to the target server to check whether the restore can be kept out of the '
                . 'binary log, so myloader will try: ' . $this->host . ':' . $this->port . ' - ' . $e->getMessage()
            );
            return true;
        }

        try {
            $connection->exec('SET SESSION SQL_LOG_BIN = 0');
        } catch (PDOException $e) {
            return false;
        }

        // Nothing else runs on this connection, so leaving the session's binlog off changes nothing.
        return true;
    }

    private function connect(): PDO
    {
        if (!isset($this->connection)) {
            $this->connection = new PDO(
                "mysql:host={$this->host};port={$this->port};charset=UTF8;",
                $this->username,
                $this->password,
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION] + $this->tls->pdoOptions()
            );
        }

        return $this->connection;
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }
}
