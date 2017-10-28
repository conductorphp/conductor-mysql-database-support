<?php

namespace DevopsToolMySqlSupport;

return [
    'factories' => [
        \DevopsToolCore\Database\DatabaseMetadataProviderInterface::class => DatabaseMetaDataProviderFactory::class,
    ],
];
