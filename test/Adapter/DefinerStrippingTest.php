<?php

namespace ConductorMySqlSupportTest\Adapter;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Adapter\Mysqldump\ExportPlugin as MysqldumpExportPlugin;
use ConductorMySqlSupport\Adapter\TabDelimited\ExportPlugin as TabDelimitedExportPlugin;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/**
 * CTAP-1607. Definer stripping had two defects that were invisible to any test that only inspected
 * the command string, so these run the emitted sed instead of reading it.
 *
 * 1. The Mysqldump and TabDelimited adapters emitted `sed "s|DEFINER=[^ ]+ |...|g"` with no `-r`.
 *    Without it sed uses a BASIC regular expression, in which `+` is a literal plus character, so
 *    the pattern could never match a real definer and the rewrite was a silent no-op.
 * 2. The option was read as `empty($options['remove_definers'])`, so `remove_definers => true`
 *    DISABLED removal. DatabaseExportCommand sends `true` whenever `--no-remove-definers` is absent,
 *    which meant the CLI stripped nothing by default on every adapter.
 *
 * 🚨 Both defects fail silently and produce a *plausible* command. A test asserting the command
 * "contains sed" or "contains DEFINER=CURRENT_USER" passes against the broken version — the string
 * was always right, only its behavior was wrong. So the assertion has to be the rewritten output.
 */
class DefinerStrippingTest extends TestCase
{
    /** A definer exactly as mysqldump renders one, backticks and all. */
    private const DEFINER_LINE = 'CREATE TRIGGER DEFINER=`dev`@`%` foo';
    private const STRIPPED     = 'CREATE TRIGGER DEFINER=CURRENT_USER foo';

    /** @return iterable<string, array{callable(array): string}> */
    public static function pipeAdapters(): iterable
    {
        yield 'Mysqldump' => [static function (array $options): string {
            $plugin = new MysqldumpExportPlugin(
                self::createShellAdapterStub(),
                'user',
                'password',
            );

            return self::dumpStructureCommand($plugin, $options);
        }];

        // TabDelimited is not named in the ticket but carries the identical broken sed.
        yield 'TabDelimited' => [static function (array $options): string {
            $plugin = new TabDelimitedExportPlugin(
                self::createShellAdapterStub(),
                'user',
                'password',
            );

            return self::dumpStructureCommand($plugin, $options);
        }];
    }

    /**
     * The regression test proper: take the sed the adapter actually emits and push a real definer
     * through it.
     *
     */
    #[DataProvider('pipeAdapters')]
    public function testTheEmittedSedRewritesARealDefiner(callable $build): void
    {
        $command = $build(['remove_definers' => true]);

        self::assertSame(self::STRIPPED, $this->runPipeline($command));
    }

    /**
     * Polarity, stated as behavior rather than as a string: asking to remove definers must remove
     * them, and asking to keep them must keep them. Under the old `empty()` read the first of these
     * failed — which is the shape a real caller hits, because the CLI always sends an explicit true.
     *
     */
    #[DataProvider('pipeAdapters')]
    public function testTheOptionIsNotInverted(callable $build): void
    {
        self::assertSame(
            self::STRIPPED,
            $this->runPipeline($build(['remove_definers' => true])),
            'remove_definers => true must REMOVE definers',
        );

        self::assertSame(
            self::DEFINER_LINE,
            $this->runPipeline($build(['remove_definers' => false])),
            'remove_definers => false must KEEP definers',
        );
    }

    /**
     * An omitted option keeps stripping, so a direct API caller that never passed one is unaffected
     * by the polarity fix. Only an explicit false changes anything.
     *
     */
    #[DataProvider('pipeAdapters')]
    public function testOmittingTheOptionStillStrips(callable $build): void
    {
        self::assertSame(self::STRIPPED, $this->runPipeline($build([])));
    }

    /**
     * Run the `| sed ...` tail of the emitted command against a real definer line.
     *
     * When the adapter emits no rewrite at all (definers kept) there is no pipe, and the input comes
     * back untouched — which is the correct expectation for that case.
     */
    private function runPipeline(string $command): string
    {
        $pipe = strstr($command, '| sed');

        if ($pipe === false) {
            return self::DEFINER_LINE;
        }

        $shell = 'printf %s ' . escapeshellarg(self::DEFINER_LINE) . ' ' . $pipe;

        return trim((string) shell_exec($shell));
    }

    /** @param array<string, mixed> $options */
    private static function dumpStructureCommand(object $plugin, array $options): string
    {
        // No setAccessible(): it is a no-op since PHP 8.1 and deprecated on 8.5.
        return (string) (new ReflectionMethod($plugin, 'getDumpStructureCommand'))
            ->invoke($plugin, 'mydb', $options);
    }

    private static function createShellAdapterStub(): ShellAdapterInterface
    {
        return new class implements ShellAdapterInterface {
            public function isCallable(string $command): bool
            {
                return true;
            }

            public function runShellCommand(
                string $command,
                ?string $currentWorkingDirectory = null,
                ?array $environmentVariables = null,
                int $priority = self::PRIORITY_NORMAL,
                ?array $options = null
            ): string {
                return '';
            }
        };
    }
}
