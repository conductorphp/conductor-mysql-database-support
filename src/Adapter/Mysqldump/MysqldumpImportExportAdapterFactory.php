<?php

namespace ConductorMySqlSupport\Adapter\Mysqldump;

use ConductorMySqlSupport\Adapter\Mydumper\MydumperImportExportAdapter;
use ConductorMySqlSupport\Exception;
use Laminas\ServiceManager\Factory\FactoryInterface;
use Psr\Container\ContainerInterface;

class MysqldumpImportExportAdapterFactory implements FactoryInterface
{
    public function __invoke(ContainerInterface $container, $requestedName, ?array $options = null): MysqldumpImportExportAdapter
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
