<?php

namespace ConductorMySqlSupport\Adapter\Mydumper;

use ConductorCore\Exception\ShellCommandFailedException;

/**
 * Reads what mydumper and myloader write to stderr.
 *
 * Both log through glib, one line per event: "** (mydumper:736): WARNING **: 15:04:55.544: <message>".
 * At -v 3 nearly every line is a Message, so a failure's whole stderr is mostly progress; the
 * WARNING, CRITICAL and ERROR lines are the ones that say what went wrong.
 */
final class MydumperLog
{
    /**
     * The level has to be exactly WARNING, CRITICAL or ERROR, so glib's own "GLib-CRITICAL" chatter,
     * which myloader emits on a healthy run too, is not taken for a problem.
     */
    private const PROBLEM_LINE = '/^\*\* \([^)]+\): (WARNING|CRITICAL|ERROR) \*\*: [0-9:.]+: (.*)$/';

    private const MAX_REPORTED_PROBLEMS = 10;

    /** Lines of stderr reported when it holds no problem line at all. */
    private const TAIL_LINES = 20;

    /**
     * @return list<array{0: string, 1: string}> Each problem line as [level, message], in order
     */
    public static function problems(string $log): array
    {
        $problems = [];
        foreach (preg_split('/\R/', $log) as $line) {
            if (preg_match(self::PROBLEM_LINE, $line, $matches)) {
                $problems[] = [$matches[1], trim($matches[2])];
            }
        }

        return $problems;
    }

    /**
     * The shell adapter's message is the command and its stdout, and mydumper and myloader write
     * nothing useful to stdout, so a failure read "Output:" and then nothing. This adds the problem
     * lines from stderr, or, when there are none, the end of it.
     */
    public static function describeFailure(\Throwable $exception): string
    {
        $message = $exception->getMessage();
        if (!$exception instanceof ShellCommandFailedException) {
            return $message;
        }

        $stderr = trim($exception->getStderr());
        if ('' === $stderr) {
            return $message;
        }

        $problems = [];
        foreach (self::problems($stderr) as [$level, $problem]) {
            $problems["$level: $problem"] = "$level: $problem";
        }
        if ($problems) {
            return $message . "\nStderr (warnings and errors):\n  "
                . implode("\n  ", array_slice(array_values($problems), 0, self::MAX_REPORTED_PROBLEMS));
        }

        $lines = preg_split('/\R/', $stderr);

        return $message . "\nStderr:\n" . implode("\n", array_slice($lines, -self::TAIL_LINES));
    }
}
