<?php

namespace ConductorMySqlSupport\Adapter;

use ConductorMySqlSupport\Exception;
use Laminas\ServiceManager\Factory\FactoryInterface;
use Psr\Container\ContainerInterface;

class DatabaseAdapterFactory implements FactoryInterface
{
    public function __invoke(ContainerInterface $container, $requestedName, ?array $options = null): DatabaseAdapter
    {
        $this->validateOptions($options);

        return new DatabaseAdapter(
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
                    DatabaseAdapter::class,
                    implode(', ', $missingRequiredOptions)
                )
            );
        }

        $disallowedOptions = array_diff(array_keys($options), $allowedOptions);
        if ($disallowedOptions) {
            throw new Exception\InvalidArgumentException(
                sprintf(
                    'Invalid %s constructor options: %s',
                    DatabaseAdapter::class,
                    implode(', ', $disallowedOptions)
                )
            );
        }
    }
}
