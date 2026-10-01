<?php

namespace ConductorMySqlSupport\Adapter\TabDelimited;

use Psr\Log\LoggerInterface;
use ConductorMySqlSupport\Adapter\TlsOptions;
use ConductorMySqlSupport\Exception;
use Laminas\ServiceManager\Factory\FactoryInterface;
use Psr\Container\ContainerInterface;

class TabDelimitedImportExportAdapterFactory implements FactoryInterface
{
    public function __invoke(ContainerInterface $container, $requestedName, ?array $options = null): TabDelimitedImportExportAdapter
    {
        $this->validateOptions($options);

        $tls = TlsOptions::fromOptions($options);
        if ($container->has(LoggerInterface::class)) {
            $tls->warnIfUnverified($container->get(LoggerInterface::class));
        }

        return new TabDelimitedImportExportAdapter(
            $options['username'],
            $options['password'],
            $options['host'] ?? null,
            $options['port'] ?? null,
            tls: $tls
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
                    TabDelimitedImportExportAdapter::class,
                    implode(', ', $missingRequiredOptions)
                )
            );
        }

        $disallowedOptions = array_diff(array_keys($options), $allowedOptions);
        if ($disallowedOptions) {
            throw new Exception\InvalidArgumentException(
                sprintf(
                    'Invalid %s constructor options: %s',
                    TabDelimitedImportExportAdapter::class,
                    implode(', ', $disallowedOptions)
                )
            );
        }
    }
}
