<?php

namespace ConductorMySqlSupport\Adapter;

use Psr\Log\LoggerInterface;
use ConductorMySqlSupport\Exception;
use Laminas\ServiceManager\Factory\FactoryInterface;
use Psr\Container\ContainerInterface;

class DatabaseAdapterFactory implements FactoryInterface
{
    public function __invoke(ContainerInterface $container, $requestedName, ?array $options = null): DatabaseAdapter
    {
        $this->validateOptions($options);

        $tls = TlsOptions::fromOptions($options);
        if ($container->has(LoggerInterface::class)) {
            $tls->warnIfUnverified($container->get(LoggerInterface::class));
        }

        return new DatabaseAdapter(
            $options['username'],
            $options['password'],
            $options['host'] ?? null,
            $options['port'] ?? null,
            $tls
        );
    }

    /**
     * @throws Exception\InvalidArgumentException if options are invalid
     */
    private function validateOptions(array $options): void
    {
        $requiredOptions = ['username', 'password'];
        $allowedOptions = ['username', 'password', 'host', 'port', ...TlsOptions::OPTION_KEYS];

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
