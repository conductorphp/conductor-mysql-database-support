<?php

namespace ConductorMySqlSupportTest\Adapter;

use ConductorCore\Shell\Adapter\ShellAdapterInterface;
use ConductorMySqlSupport\Adapter\Mydumper;
use ConductorMySqlSupport\Adapter\Mysqldump;
use ConductorMySqlSupport\Adapter\TabDelimited;
use ConductorMySqlSupport\Adapter\TlsOptions;
use ConductorMySqlSupport\Exception;
use Pdo\Mysql;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionMethod;

use function escapeshellarg;
use function file_get_contents;
use function fileperms;
use function openssl_get_cert_locations;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

/**
 * CTAP-2120. TLS for every connection the package opens. Absent, nothing changes.
 */
final class TlsOptionsTest extends TestCase
{
    #[Test]
    public function absentOptionsLeaveEveryConnectionAsItWas(): void
    {
        $tls = TlsOptions::fromOptions(['username' => 'u', 'password' => 'p']);

        self::assertFalse($tls->enabled);
        self::assertSame([], $tls->pdoOptions());
        self::assertSame('', $tls->mysqlClientArguments());
        self::assertSame('', $tls->mydumperArguments());
    }

    /**
     * Values arrive through `${VAR:-}` interpolation, so they are strings.
     *
     * @return iterable<string, array{mixed, bool}>
     */
    public static function interpolatedBooleans(): iterable
    {
        yield '"1"'    => ['1', true];
        yield '"0"'    => ['0', false];
        yield '""'     => ['', false];
        yield '"true"' => ['true', true];
        yield '"Off"'  => ['Off', false];
        yield 'int 1'  => [1, true];
        yield 'bool'   => [true, true];
    }

    #[Test]
    #[DataProvider('interpolatedBooleans')]
    public function interpolatedBooleansAreNormalized(mixed $value, bool $expected): void
    {
        self::assertSame($expected, TlsOptions::fromOptions(['tls' => $value])->enabled);
    }

    #[Test]
    public function anEmptyVerifyKeepsTheSafeDefault(): void
    {
        self::assertTrue(TlsOptions::fromOptions(['tls' => '1', 'tls_verify' => ''])->verify);
    }

    #[Test]
    public function aValueThatIsNotABooleanIsRefused(): void
    {
        $this->expectException(Exception\InvalidArgumentException::class);
        TlsOptions::fromOptions(['tls' => 'sometimes']);
    }

    /** A CA set ahead of turning TLS on must not fail every deploy in between. */
    #[Test]
    public function certificatesWithTlsOffAreIgnored(): void
    {
        $tls = TlsOptions::fromOptions(['tls' => '0', 'tls_ca' => "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----"]);

        self::assertFalse($tls->enabled);
        self::assertNull($tls->ca);
        self::assertSame('', $tls->mysqlClientArguments());
    }

    /** `tls_ca: '${DATABASE_TLS_CA|b64decode:-}'` hands the adapter the PEM; the clients get a file. */
    #[Test]
    public function pemContentBecomesAPrivateFile(): void
    {
        $pem = "-----BEGIN CERTIFICATE-----\nMIIBtest\n-----END CERTIFICATE-----";
        $tls = TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => $pem]);

        self::assertNotNull($tls->ca);
        self::assertFileExists($tls->ca);
        self::assertSame($pem . "\n", file_get_contents($tls->ca));
        self::assertSame(0600, fileperms($tls->ca) & 0777);
        self::assertSame($tls->ca, TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => $pem])->ca, 'one file per PEM');
    }

    #[Test]
    public function anEmptyCertificateMeansNone(): void
    {
        $empty = tempnam(sys_get_temp_dir(), 'ca');
        try {
            self::assertNull(TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => $empty])->ca);
            self::assertNull(TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => ''])->ca);
        } finally {
            unlink($empty);
        }
    }

    #[Test]
    public function aClientCertificateNeedsItsKey(): void
    {
        $this->expectException(Exception\InvalidArgumentException::class);
        TlsOptions::fromOptions(['tls' => '1', 'tls_cert' => '/c.pem']);
    }

    #[Test]
    public function pdoGetsTheCaAndVerification(): void
    {
        $tls = TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => '/etc/ssl/rds.pem', 'tls_cert' => '/c.pem', 'tls_key' => '/k.pem']);

        self::assertSame(
            [
                Mysql::ATTR_SSL_CA                 => '/etc/ssl/rds.pem',
                Mysql::ATTR_SSL_VERIFY_SERVER_CERT => true,
                Mysql::ATTR_SSL_CERT               => '/c.pem',
                Mysql::ATTR_SSL_KEY                => '/k.pem',
            ],
            $tls->pdoOptions(),
        );
    }

    /** mysqlnd negotiates TLS only when an SSL attribute is set; without a CA it would stay plaintext. */
    #[Test]
    public function pdoWithoutACaStillForcesTlsWithTheSystemBundle(): void
    {
        $options = TlsOptions::fromOptions(['tls' => '1', 'tls_verify' => '0'])->pdoOptions();

        self::assertNotEmpty($options[Mysql::ATTR_SSL_CA]);
        self::assertFalse($options[Mysql::ATTR_SSL_VERIFY_SERVER_CERT]);
    }

    #[Test]
    public function mysqlClientFlagsWorkForMySqlAndMariaDb(): void
    {
        $verified = TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => '/etc/ssl/rds.pem']);
        self::assertSame(
            " --loose-ssl --loose-ssl-mode=VERIFY_IDENTITY --loose-ssl-verify-server-cert --ssl-ca='/etc/ssl/rds.pem'",
            $verified->mysqlClientArguments(),
        );

        $unverified = TlsOptions::fromOptions(['tls' => '1', 'tls_verify' => '0']);
        self::assertSame(
            ' --loose-ssl --loose-ssl-mode=REQUIRED --loose-disable-ssl-verify-server-cert',
            $unverified->mysqlClientArguments(),
        );
    }

    #[Test]
    public function mydumperFlags(): void
    {
        self::assertSame(
            " --ssl --ssl-mode=VERIFY_IDENTITY --ca='/etc/ssl/rds.pem'",
            TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => '/etc/ssl/rds.pem'])->mydumperArguments(),
        );
        self::assertSame(' --ssl --ssl-mode=REQUIRED', TlsOptions::fromOptions(['tls' => '1', 'tls_verify' => '0'])->mydumperArguments());
    }

    /**
     * CTAP-2130. mysql and mydumper refuse VERIFY_IDENTITY without a CA ("SSL required option
     * missing: ca"), so an empty tls_ca gives them the system bundle, as PDO already gets.
     */
    #[Test]
    public function verifiedCommandLineFlagsWithoutACaUseTheSystemBundle(): void
    {
        $bundle = escapeshellarg(openssl_get_cert_locations()['default_cert_file']);
        $tls    = TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => '']);

        self::assertSame(
            " --loose-ssl --loose-ssl-mode=VERIFY_IDENTITY --loose-ssl-verify-server-cert --ssl-ca=$bundle",
            $tls->mysqlClientArguments(),
        );
        self::assertSame(" --ssl --ssl-mode=VERIFY_IDENTITY --ca=$bundle", $tls->mydumperArguments());
    }

    #[Test]
    public function anUnverifiedConnectionIsWarnedAboutOnce(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects(self::once())->method('warning');

        $tls = TlsOptions::fromOptions(['tls' => '1', 'tls_verify' => '0']);
        $tls->warnIfUnverified($logger);
        $tls->warnIfUnverified($logger);
    }

    /**
     * The flags reach every command each plugin builds, and nothing changes without TLS.
     *
     * @return iterable<string, array{class-string, string, string}>
     */
    public static function connectionBuilders(): iterable
    {
        yield 'mydumper'          => [Mydumper\ExportPlugin::class, 'getMysqldumperCommandConnectionArguments', 'mydumper'];
        yield 'mydumper mysql'    => [Mydumper\ExportPlugin::class, 'getMysqlCommandConnectionArguments', 'mysql'];
        yield 'myloader'          => [Mydumper\ImportPlugin::class, 'getMysqlCommandConnectionArguments', 'mydumper'];
        yield 'mysqldump'         => [Mysqldump\ExportPlugin::class, 'getCommandConnectionArguments', 'mysql'];
        yield 'mysql import'      => [Mysqldump\ImportPlugin::class, 'getMysqlCommandConnectionArguments', 'mysql'];
        yield 'tab-delimited out' => [TabDelimited\ExportPlugin::class, 'getMysqlCommandConnectionArguments', 'mysql'];
        yield 'tab-delimited in'  => [TabDelimited\ImportPlugin::class, 'getMysqlCommandConnectionArguments', 'mysql'];
    }

    /**
     * @param class-string $plugin
     */
    #[Test]
    #[DataProvider('connectionBuilders')]
    public function everyCommandCarriesTheFlags(string $plugin, string $builder, string $flavor): void
    {
        $shell = $this->createStub(ShellAdapterInterface::class);
        $tls   = TlsOptions::fromOptions(['tls' => '1', 'tls_ca' => '/ca.pem']);
        $args  = new ReflectionMethod($plugin, $builder);

        $plain   = $args->invoke($this->plugin($plugin, $shell, null));
        $secured = $args->invoke($this->plugin($plugin, $shell, $tls));

        self::assertStringNotContainsString('ssl', $plain);
        self::assertSame(
            $plain . ($flavor === 'mydumper' ? $tls->mydumperArguments() : $tls->mysqlClientArguments()),
            $secured,
        );
    }

    private function plugin(string $plugin, ShellAdapterInterface $shell, ?TlsOptions $tls): object
    {
        return $plugin === Mydumper\ImportPlugin::class
            ? new $plugin($shell, 'user', 'secret', 'db.example.com', 3306, null, null, null, $tls)
            : new $plugin($shell, 'user', 'secret', 'db.example.com', 3306, null, $tls);
    }
}
