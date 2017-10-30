<?php

namespace DevopsToolMySqlSupport;

return [
    'factories' => [
        \DevopsToolCore\Database\DatabaseMetadataProviderInterface::class => DatabaseMetaDataProviderFactory::class,
        Adapter\Mydumper\MydumperExportAdapter::class => Adapter\Mydumper\MydumperExportAdapterFactory::class,
        Adapter\Mydumper\MydumperImportAdapter::class => Adapter\Mydumper\MydumperImportAdapterFactory::class,
        Adapter\Mysqldump\MysqldumpExportAdapter::class => Adapter\Mysqldump\MysqldumpExportAdapterFactory::class,
        Adapter\Mysqldump\MysqldumpImportAdapter::class => Adapter\Mysqldump\MysqldumpImportAdapterFactory::class,
        Adapter\TabDelimited\MysqlTabDelimitedExportAdapter::class => Adapter\TabDelimited\MysqlTabDelimitedExportAdapterFactory::class,
        Adapter\TabDelimited\MysqlTabDelimitedImportAdapter::class => Adapter\TabDelimited\MysqlTabDelimitedImportAdapterFactory::class,
    ],
];
