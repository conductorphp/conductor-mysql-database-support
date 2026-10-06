<?php

namespace ConductorMySqlSupport\Adapter;

use function escapeshellarg;
use function getenv;
use function sprintf;

/**
 * How the command-line clients (mysql, mysqldump, mysqlimport, mydumper, myloader) are told who to
 * connect as.
 *
 * The password never goes on the command line. A command line is visible to every user on the host
 * through ps, and it is what the shell adapter reports when the command fails: a failed snapshot
 * printed `-p'<password>'` into the exception, and from there into the app log, cron mail and log
 * shipping (CTAP-2218). It goes in the child's environment as MYSQL_PWD instead, which only the same
 * user can read back. Every one of these clients reads it: verified against the MariaDB 11.8 clients
 * and mydumper/myloader 1.0.5 in the conductor image; MySQL's own clients still honor it (deprecated
 * since 8.0, not removed).
 *
 * ⚠️ MYSQL_PWD is the client library's lowest-precedence source: a `password` in an option file the
 * client reads (`~/.my.cnf`, a `[client]` group in /etc/mysql) now wins over the configured one,
 * where `-p` used to win over it. `--defaults-extra-file` would not change that, since `~/.my.cnf` is
 * read after it. A deploy user with a stale password in `~/.my.cnf` has to drop it.
 */
final readonly class ClientCredentials
{
    public const PASSWORD_ENV_VAR = 'MYSQL_PWD';

    public function __construct(
        private string $username,
        private string $password,
        private string $host,
        private int $port,
    ) {
    }

    /**
     * `-h … -P … -u …`, without the password.
     */
    public function connectionArguments(): string
    {
        return sprintf(
            '-h %s -P %s -u %s',
            escapeshellarg($this->host),
            escapeshellarg((string) $this->port),
            escapeshellarg($this->username)
        );
    }

    /**
     * The environment to run a client with: conductor's own plus MYSQL_PWD. Null without a password,
     * so the command inherits conductor's environment exactly as it did before.
     *
     * @return array<string, string>|null
     */
    public function environment(): ?array
    {
        if ($this->password === '') {
            return null;
        }

        $environment = getenv();
        $environment[self::PASSWORD_ENV_VAR] = $this->password;

        return $environment;
    }
}
