<?php

namespace ConductorMySqlSupport\Adapter;

use ConductorMySqlSupport\Exception;
use Pdo\Mysql;
use Psr\Log\LoggerInterface;

use function chmod;
use function escapeshellarg;
use function file_put_contents;
use function filesize;
use function hash;
use function implode;
use function in_array;
use function is_bool;
use function is_file;
use function is_int;
use function is_readable;
use function is_string;
use function openssl_get_cert_locations;
use function register_shutdown_function;
use function sprintf;
use function str_contains;
use function strtolower;
use function sys_get_temp_dir;
use function tempnam;
use function trim;
use function unlink;

/**
 * TLS for every connection this package opens: the PDO adapter, the mysql/mysqldump clients and
 * mydumper/myloader (CTAP-2120).
 *
 * Configured with five optional adapter arguments, all off by default so an adapter without them
 * connects exactly as before:
 *
 * - `tls`        bool, default false: encrypt the connection. Once on, never falls back to plaintext.
 * - `tls_ca`     path, optional: the CA bundle to trust. Empty means the system trust store.
 * - `tls_cert`   path, optional: client certificate.
 * - `tls_key`    path, optional: client key.
 * - `tls_verify` bool, default true: verify the server's chain and host name.
 *
 * Values usually arrive through `${VAR:-}` interpolation, so the strings "0", "1", "" (and "true",
 * "false", "yes", "no", "on", "off") are accepted for the booleans. A certificate is a path or the
 * PEM itself, which is how it comes from the environment: `'${DATABASE_TLS_CA|b64decode:-}'`, a
 * base64 PEM like the JWT keys. Empty means none. With `tls` off the certificates are ignored.
 */
final readonly class TlsOptions
{
    /** The adapter argument names, for the factories' allowed-options lists. */
    public const OPTION_KEYS = ['tls', 'tls_ca', 'tls_cert', 'tls_key', 'tls_verify'];

    private const TRUE_STRINGS  = ['1', 'true', 'yes', 'on'];
    private const FALSE_STRINGS = ['0', 'false', 'no', 'off', ''];

    public function __construct(
        public bool $enabled = false,
        public ?string $ca = null,
        public ?string $cert = null,
        public ?string $key = null,
        public bool $verify = true,
    ) {
        $hasCert = $cert !== null;
        $hasKey  = $key !== null;
        if ($hasCert !== $hasKey) {
            throw new Exception\InvalidArgumentException('"tls_cert" and "tls_key" must be set together.');
        }
    }

    public static function disabled(): self
    {
        return new self();
    }

    /**
     * @param array<string, mixed> $options An adapter's arguments; keys other than OPTION_KEYS are ignored.
     */
    public static function fromOptions(array $options): self
    {
        // Off means off: certificates set ahead of turning TLS on are ignored, and no file is written.
        if (!self::toBool('tls', $options['tls'] ?? null, false)) {
            self::toBool('tls_verify', $options['tls_verify'] ?? null, true);

            return self::disabled();
        }

        return new self(
            enabled: true,
            ca: self::toPath($options['tls_ca'] ?? null),
            cert: self::toPath($options['tls_cert'] ?? null),
            key: self::toPath($options['tls_key'] ?? null),
            verify: self::toBool('tls_verify', $options['tls_verify'] ?? null, true),
        );
    }

    /**
     * PDO driver options. Empty when TLS is off, so the connection is opened exactly as before.
     *
     * mysqlnd only negotiates TLS when an SSL attribute is set, so a CA is always passed once TLS is
     * on, the system bundle when none is configured: without one, the connection would silently
     * stay plaintext.
     *
     * @return array<int, mixed>
     */
    public function pdoOptions(): array
    {
        if (!$this->enabled) {
            return [];
        }

        $options = [
            Mysql::ATTR_SSL_CA                 => $this->ca ?? self::systemCaBundle(),
            Mysql::ATTR_SSL_VERIFY_SERVER_CERT => $this->verify,
        ];
        if ($this->cert !== null && $this->key !== null) {
            $options[Mysql::ATTR_SSL_CERT] = $this->cert;
            $options[Mysql::ATTR_SSL_KEY]  = $this->key;
        }

        return $options;
    }

    /**
     * Flags for the mysql and mysqldump clients, with a leading space, or "" when TLS is off.
     *
     * The clients on a deploy host may be MySQL's or MariaDB's, and their TLS flags differ: MySQL 8
     * has only `--ssl-mode`, MariaDB has `--ssl` and `--ssl-verify-server-cert` and no `--ssl-mode`.
     * Each flavor-specific flag carries the `--loose-` prefix, which both clients honor by ignoring
     * an option they do not know (with a warning on stderr, which conductor logs at debug). The path
     * flags are common to both. Without a CA the client's own default trust store is used: these
     * commands may run on another host, where this process's bundle path means nothing.
     */
    public function mysqlClientArguments(): string
    {
        if (!$this->enabled) {
            return '';
        }

        $flags = $this->verify
            ? ['--loose-ssl', '--loose-ssl-mode=VERIFY_IDENTITY', '--loose-ssl-verify-server-cert']
            : ['--loose-ssl', '--loose-ssl-mode=REQUIRED', '--loose-disable-ssl-verify-server-cert'];
        foreach (['--ssl-ca' => $this->ca, '--ssl-cert' => $this->cert, '--ssl-key' => $this->key] as $flag => $path) {
            if ($path !== null) {
                $flags[] = $flag . '=' . escapeshellarg($path);
            }
        }

        return ' ' . implode(' ', $flags);
    }

    /**
     * Flags for mydumper and myloader, with a leading space, or "" when TLS is off. Their flags are
     * the same whichever client library they were built against.
     */
    public function mydumperArguments(): string
    {
        if (!$this->enabled) {
            return '';
        }

        $flags = ['--ssl', '--ssl-mode=' . ($this->verify ? 'VERIFY_IDENTITY' : 'REQUIRED')];
        foreach (['--ca' => $this->ca, '--cert' => $this->cert, '--key' => $this->key] as $flag => $path) {
            if ($path !== null) {
                $flags[] = $flag . '=' . escapeshellarg($path);
            }
        }

        return ' ' . implode(' ', $flags);
    }

    /**
     * Logs, once per process, that the connection is encrypted but the server is not verified, so a
     * man in the middle would go unnoticed. Called by the factories.
     */
    public function warnIfUnverified(LoggerInterface $logger): void
    {
        static $warned = false;
        if ($warned || !$this->enabled || $this->verify) {
            return;
        }
        $warned = true;
        $logger->warning(
            'MySQL TLS is on with tls_verify=0: connections are encrypted, but the server certificate '
            . 'and host name are not checked.'
        );
    }

    private static function systemCaBundle(): string
    {
        $bundle = openssl_get_cert_locations()['default_cert_file'] ?? '';
        if (!is_string($bundle) || $bundle === '' || !is_readable($bundle)) {
            throw new Exception\RuntimeException(sprintf(
                'MySQL TLS is on without "tls_ca", and the system CA bundle "%s" cannot be read. Set "tls_ca".',
                (string) $bundle,
            ));
        }

        return $bundle;
    }

    private static function toBool(string $name, mixed $value, bool $default): bool
    {
        if ($value === null) {
            return $default;
        }
        if (is_bool($value)) {
            return $value;
        }
        if (is_int($value) && in_array($value, [0, 1], true)) {
            return $value === 1;
        }
        if (is_string($value)) {
            $normalized = strtolower(trim($value));
            if ($normalized === '') {
                return $default;
            }
            if (in_array($normalized, self::TRUE_STRINGS, true)) {
                return true;
            }
            if (in_array($normalized, self::FALSE_STRINGS, true)) {
                return false;
            }
        }

        throw new Exception\InvalidArgumentException(sprintf(
            '"%s" must be a boolean (1/0, true/false, yes/no, on/off); got %s.',
            $name,
            var_export($value, true),
        ));
    }

    /**
     * A certificate argument is a path, or the PEM itself. The PEM form is how a project passes it
     * from the environment, base64-encoded like the JWT keys: `tls_ca: '${DATABASE_TLS_CA|b64decode:-}'`.
     * The clients need a file, so PEM content is written to a private temporary file, removed when
     * the process exits. Empty, or a path to an empty file, means none.
     */
    private static function toPath(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }
        if (!is_string($value)) {
            throw new Exception\InvalidArgumentException('TLS certificates must be a path or PEM content.');
        }
        $value = trim($value);
        if ($value === '') {
            return null;
        }
        if (str_contains($value, '-----BEGIN ')) {
            return self::pemFile($value);
        }
        if (is_file($value) && filesize($value) === 0) {
            return null;
        }

        return $value;
    }

    /**
     * One file per distinct PEM per process, readable by this user only (a client key may be
     * among them). The commands that read it must run on this host: a shell adapter that runs them
     * elsewhere needs a path that exists there.
     */
    private static function pemFile(string $pem): string
    {
        static $files = [];
        $hash = hash('sha256', $pem);
        if (isset($files[$hash]) && is_file($files[$hash])) {
            return $files[$hash];
        }

        $file = tempnam(sys_get_temp_dir(), 'conductor-tls-');
        if ($file === false || file_put_contents($file, $pem . "\n") === false || !chmod($file, 0600)) {
            throw new Exception\RuntimeException('Could not write the TLS certificate to a temporary file.');
        }
        if ($files === []) {
            register_shutdown_function(static function () use (&$files): void {
                foreach ($files as $path) {
                    @unlink($path);
                }
            });
        }
        $files[$hash] = $file;

        return $file;
    }
}
