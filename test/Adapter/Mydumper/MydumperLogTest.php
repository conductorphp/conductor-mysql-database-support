<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorCore\Exception\ShellCommandFailedException;
use ConductorMySqlSupport\Adapter\Mydumper\MydumperLog;
use PHPUnit\Framework\TestCase;

class MydumperLogTest extends TestCase
{
    private const STDERR = <<<STDERR
        ** Message: 15:04:55.525: MyDumper backup version: 1.0.5-1
        (myloader:778): GLib-CRITICAL **: 15:05:25.825: g_key_file_get_groups: assertion 'key_file != NULL' failed

        ** (mydumper:736): WARNING **: 15:04:55.544: Couldn't get master position - ERROR 1227: Access denied
        ** Message: 15:04:55.547: Thread 3: dumping db information for `mydb`
        ** (mydumper:736): CRITICAL **: 15:04:55.556: Could not write schema for `mydb`.`t`
        ** (mydumper:736): WARNING **: 15:04:55.557: Couldn't get master position - ERROR 1227: Access denied

        STDERR;

    public function testReadsTheWarningsAndErrorsButNotGlibsOwnChatter(): void
    {
        $this->assertSame(
            [
                ['WARNING', "Couldn't get master position - ERROR 1227: Access denied"],
                ['CRITICAL', 'Could not write schema for `mydb`.`t`'],
                ['WARNING', "Couldn't get master position - ERROR 1227: Access denied"],
            ],
            MydumperLog::problems(self::STDERR)
        );
    }

    /**
     * mydumper writes nothing to stdout, so the shell adapter's message ended "Output:" and nothing
     * else, and the reason was lost (CTAP-2267).
     */
    public function testAddsTheProblemsOnStderrToTheFailure(): void
    {
        $message = MydumperLog::describeFailure(new ShellCommandFailedException('mydumper …', 1, '', self::STDERR));

        $this->assertStringContainsString("Output: \nStderr (warnings and errors):\n", $message);
        $this->assertSame(
            1,
            substr_count($message, "WARNING: Couldn't get master position - ERROR 1227: Access denied"),
            'A problem repeated is reported once.'
        );
        $this->assertStringContainsString('CRITICAL: Could not write schema for `mydb`.`t`', $message);
        $this->assertStringNotContainsString('dumping db information', $message);
    }

    public function testAddsTheEndOfStderrWhenItHasNoProblemLines(): void
    {
        $message = MydumperLog::describeFailure(
            new ShellCommandFailedException('mydumper …', 2, '', "mydumper: unrecognized option '--nope'\n")
        );

        $this->assertStringEndsWith("\nStderr:\nmydumper: unrecognized option '--nope'", $message);
    }

    public function testLeavesAFailureWithNothingOnStderrAlone(): void
    {
        $failure = new ShellCommandFailedException('mydumper …', 1);

        $this->assertSame($failure->getMessage(), MydumperLog::describeFailure($failure));
    }

    public function testLeavesAnyOtherExceptionAlone(): void
    {
        $this->assertSame('boom', MydumperLog::describeFailure(new \RuntimeException('boom')));
    }
}
