<?php

namespace ConductorMySqlSupport\Adapter\Mysqldump;

use ConductorMySqlSupport\Adapter\Mydumper\MydumperImportExportAdapter;
use ConductorMySqlSupport\Exception;
use Interop\Container\ContainerInterface;
use Interop\Container\Exception\ContainerException;
use Laminas\ServiceManager\Exception\ServiceNotCreatedException;
use Laminas\ServiceManager\Exception\ServiceNotFoundException;
use Laminas\ServiceManager\Factory\FactoryInterface;

class MysqldumpImportExportAdapterFactory implements FactoryInterface
{
    public function __invoke(\Psr\Container\ContainerInterface $container, $requestedName, ?array $options = null): MysqldumpImportExportAdapter
    {
        $this->validateOptions($options);

        return new MysqldumpImportExportAdapter(
            $options['username'],
            $options['password'],
            $options['host'] ?? null,
            $options['port'] ?? null
        );
    }

    /**
     * @throws Exception\InvalidArgumentException if options are invalid
     */
    private function validateOptions(array $options): void
    {
        $requiredOptions = ['username', 'password'];
        $allowedOptions = ['username', 'password', 'host', 'port'];

        $missingRequiredOptions = array_diff($requiredOptions, array_keys($options));
        if ($missingRequiredOptions) {
            throw new Exception\InvalidArgumentException(
                sprintf(
                    'Missing %s constructor options: %s',
                    MydumperImportExportAdapter::class,
                    implode(', ', $missingRequiredOptions)
                )
            );
        }

        $disallowedOptions = array_diff(array_keys($options), $allowedOptions);
        if ($disallowedOptions) {
            throw new Exception\InvalidArgumentException(
                sprintf(
                    'Invalid %s constructor options: %s',
                    MydumperImportExportAdapter::class,
                    implode(', ', $disallowedOptions)
                )
            );
        }
    }
}
