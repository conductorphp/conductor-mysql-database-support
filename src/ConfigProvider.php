<?php

namespace DevopsToolMySqlSupport;

class ConfigProvider
{
    /**
     * Returns the configuration array
     *
     * To add a bit of a structure, each section is defined in a separate
     * method which returns an array with its configuration.
     *
     * @return array
     */
    public function __invoke()
    {
        return [
            'database' => $this->getDatabaseConfig(),
            'dependencies' => $this->getDependencies(),
        ];
    }

    /**
     * Returns the container dependencies
     *
     * @return array
     */
    private function getDependencies(): array
    {
        return require(__DIR__ . '/../config/dependencies.php');
    }

    /**
     * Returns the container dependencies
     *
     * @return array
     */
    private function getDatabaseConfig(): array
    {
        return require(__DIR__ . '/../config/database.php');
    }

}
