<?php

namespace DevopsToolMySqlSupport;

return [
    'factories' => [
        \DevopsToolCore\Database\DatabaseMetaDataProviderInterface::class => DatabaseMetaDataProviderFactory::class,
    ],
];
