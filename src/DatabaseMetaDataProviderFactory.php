<?php

namespace DevopsToolMySqlSupport;

class DatabaseMetaDataProviderFactory implements \Zend\ServiceManager\Factory\FactoryInterface
{

    /**
     * Create an object
     *
     * @param  \Interop\Container\ContainerInterface $container
     * @param  string                                $requestedName
     * @param  null|array                            $options
     *
     * @return object
     * @throws \Zend\ServiceManager\Exception\ServiceNotFoundException if unable to resolve the service.
     * @throws \Zend\ServiceManager\Exception\ServiceNotCreatedException if an exception is raised when
     *     creating a service.
     * @throws \Interop\Container\Exception\ContainerException if any other error occurs
     */
    public function __invoke(\Interop\Container\ContainerInterface $container, $requestedName, array $options = null)
    {
        $config = $container->get('config')['mysql'];
        $connection = new \PDO("mysql:host={$config['host']}", $config['user'], $config['password']);
        return new DatabaseMetadataProvider($connection);
    }
}
