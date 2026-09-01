<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorMySqlSupport\Exception;

/**
 * The mydumper version floor this adapter is built against.
 *
 * mydumper 1.0 renamed the loader options we drive: `--overwrite-tables` became `-o, --drop-table`
 * and `--purge-mode` was folded into it. There is no spelling of "drop the table first" that both
 * 0.x and 1.0 accept as a long option, and a restore that silently skipped the drop would be far
 * worse than one that refuses to start — so this is a hard floor rather than a compatibility shim.
 *
 * The check is worth its one extra process: without it, an image still carrying 0.19 fails deep in
 * a restore with "option parsing failed", naming a flag the operator never typed.
 */
class MydumperVersion
{
    /**
     * Both binaries ship from the same package, so the floor covers mydumper and myloader alike.
     */
    public const MINIMUM_VERSION = '1.0.5';

    /**
     * Both report as "myloader v1.0.5-1, built against MySQL 8.4.11 with SSL support". The trailing
     * "-1" is the package revision, not part of the upstream version, so it is not compared.
     */
    private const VERSION_PATTERN = '/\bv?([0-9]+\.[0-9]+\.[0-9]+)/';

    public static function parse(string $versionOutput): ?string
    {
        if (!preg_match(self::VERSION_PATTERN, $versionOutput, $matches)) {
            return null;
        }

        return $matches[1];
    }

    public static function isSupported(string $version): bool
    {
        return version_compare($version, self::MINIMUM_VERSION, '>=');
    }

    /**
     * @throws Exception\RuntimeException If $binary is older than the floor, or will not say
     */
    public static function assertSupported(string $binary): void
    {
        $output = [];
        exec(escapeshellarg($binary) . ' --version 2>&1', $output, $return);
        $versionOutput = trim(implode("\n", $output));

        $version = self::parse($versionOutput);
        if (null === $version) {
            throw new Exception\RuntimeException(
                sprintf('the %s version could not be determined from "%s"', $binary, $versionOutput)
            );
        }

        if (!self::isSupported($version)) {
            // Deliberately not "so this command cannot run" — that is only true of myloader. mydumper
            // 0.x still exports fine; the reason to refuse it is that the myloader beside it cannot
            // restore the result, and a snapshot you cannot restore is worth failing early over.
            throw new Exception\RuntimeException(
                sprintf(
                    '%1$s %2$s is installed but mydumper %3$s or newer is required. mydumper 1.0 renamed '
                    . 'the loader options this adapter drives, and both binaries ship from one package, so '
                    . 'an older package cannot restore what it exports. Update mydumper in the image this '
                    . 'runs in',
                    $binary,
                    $version,
                    self::MINIMUM_VERSION
                )
            );
        }
    }
}
