<?php

return [
    'host' => 'localhost',
    'port' => 3306,
    'export_adapters' => [
        'mydumper' => \DevopsToolMySqlSupport\Adapter\Mydumper\MydumperExportAdapter::class,
        'mysqldump' => \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpExportAdapter::class,
        'tab' => \DevopsToolMySqlSupport\Adapter\TabDelimited\MysqlTabDelimitedExportAdapter::class,
    ],
    'import_adapters' => [
        'mydumper' => \DevopsToolMySqlSupport\Adapter\Mydumper\MydumperImportAdapter::class,
        'mysqldump' => \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpImportAdapter::class,
        'tab' => \DevopsToolMySqlSupport\Adapter\TabDelimited\MysqlTabDelimitedImportAdapter::class,
    ],
];
