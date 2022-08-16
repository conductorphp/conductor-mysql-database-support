<?php

namespace ConductorMySqlSupport\Adapter;

use ConductorMySqlSupport\Exception;

class DatabaseConfig
{
    public string $user;
    public string $password;
    public string $host;
    public string $port;

    public function __construct(string $username, string $password, string $host = 'localhost', int $port = 3306)
    {
        $this->user = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
    }

    public static function createFromArray(array $config): static
    {
        $validKeys = ['user', 'password', 'host', 'port'];
        $invalidKeys = array_diff(array_keys($config), $validKeys);
        if ($invalidKeys) {
            throw new Exception\InvalidArgumentException(
                'Invalid key(s) "' . implode('", "', $invalidKeys) . '" provided.'
            );
        }

        if (empty($config['user']) || empty($config['password'])) {
            throw new Exception\InvalidArgumentException('Keys "username" and "password" must not be empty.');
        }

        $user = $config['user'];
        $password = $config['password'];
        $host = $config['host'] ?? 'localhost';
        $port = $config['port'] ?? 3306;
        return new static($user, $password, $host, $port);
    }
}
