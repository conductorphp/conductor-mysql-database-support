<?php

namespace DevopsToolMySqlSupport\Adapter;

use DevopsToolMySqlSupport\Exception;

class DatabaseConfig
{
    /**
     * @var string
     */
    public $user;
    /**
     * @var string
     */
    public $password;
    /**
     * @var string
     */
    public $host;
    /**
     * @var string
     */
    public $port;

    /**
     * DatabaseConfig constructor.
     *
     * @param        $username
     * @param        $password
     * @param string $host
     * @param int    $port
     */
    public function __construct($username, $password, $host = 'localhost', $port = 3306)
    {
        $this->user = $username;
        $this->password = $password;
        $this->host = $host;
        $this->port = $port;
    }

    /**
     * @param array $config
     *
     * @return DatabaseConfig
     */
    public static function createFromArray(array $config)
    {
        $validKeys = ['user', 'password', 'host', 'port'];
        $invalidKeys = array_diff(array_keys($config), $validKeys);
        if ($invalidKeys) {
            throw new Exception\InvalidArgumentException(
                'Invalid key(s) "' . implode('", "', $invalidKeys) . '"" provided.'
            );
        }

        if (empty($config['user']) || empty($config['password'])) {
            throw new Exception\InvalidArgumentException('Keys "username" and "password" must not be empty.');
        }

        $user = $config['user'];
        $password = $config['password'];
        $host = (isset($config['host'])) ? $config['host'] : 'localhost';
        $port = (isset($config['port'])) ? $config['port'] : 3306;
        return new self($user, $password, $host, $port);
    }
}
