<?php

namespace ConductorMySqlSupport;

return [
    'factories' => [
        Adapter\DatabaseAdapter::class => Adapter\DatabaseAdapterFactory::class,
        Adapter\Mydumper\MydumperImportExportAdapter::class => Adapter\Mydumper\MydumperImportExportAdapterFactory::class,
        Adapter\Mysqldump\MysqldumpImportExportAdapter::class => Adapter\Mysqldump\MysqldumpImportExportAdapterFactory::class,
        Adapter\TabDelimited\TabDelimitedImportExportAdapter::class => Adapter\TabDelimited\TabDelimitedImportExportAdapterFactory::class,
    ],
];
