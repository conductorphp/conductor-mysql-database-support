<?php

namespace DevopsToolMySqlSupport;

return [
    'factories' => [
        \DevopsToolCore\Database\DatabaseMetadataProviderInterface::class => DatabaseMetaDataProviderFactory::class,
        \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpExportAdapter::class => \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpExportAdapterFactory::class,
        \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpImportAdapter::class => \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpImportAdapterFactory::class,
    ],
];
