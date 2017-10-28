<?php

namespace DevopsToolMySqlSupport\Adapter\Mysqldump;


use DevopsToolCore\ShellCommandHelper;
use DevopsToolMySqlSupport\Adapter\DatabaseConfig;
use Interop\Container\ContainerInterface;
use Interop\Container\Exception\ContainerException;
use Psr\Log\LoggerInterface;
use Zend\ServiceManager\Exception\ServiceNotCreatedException;
use Zend\ServiceManager\Exception\ServiceNotFoundException;
use Zend\ServiceManager\Factory\FactoryInterface;

class MysqldumpImportAdapterFactory implements FactoryInterface
{

    /**
     * Create an object
     *
     * @param  ContainerInterface $container
     * @param  string             $requestedName
     * @param  null|array         $options
     *
     * @return object
     * @throws ServiceNotFoundException if unable to resolve the service.
     * @throws ServiceNotCreatedException if an exception is raised when
     *     creating a service.
     * @throws ContainerException if any other error occurs
     */
    public function __invoke(ContainerInterface $container, $requestedName, array $options = null)
    {
        $config = $container->get('config')['database'];
        $databaseConfig = new DatabaseConfig($config['user'], $config['password'], $config['host'], $config['port']);
        $shellCommandHelper = $container->get(ShellCommandHelper::class);
        $logger = $container->get(LoggerInterface::class);
        return new MysqldumpImportAdapter($databaseConfig, $shellCommandHelper, $logger);
    }
}
