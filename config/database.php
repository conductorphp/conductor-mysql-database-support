<?php

return [
    'host' => 'localhost',
    'port' => 3306,
    'export_adapters' => [
        'mysqldump' => \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpExportAdapter::class,
    ],
    'import_adapters' => [
        'mysqldump' => \DevopsToolMySqlSupport\Adapter\Mysqldump\MysqldumpImportAdapter::class,
    ],
];
