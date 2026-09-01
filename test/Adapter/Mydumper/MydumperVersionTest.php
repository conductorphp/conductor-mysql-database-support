<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorMySqlSupport\Adapter\Mydumper\MydumperVersion;
use PHPUnit\Framework\TestCase;

class MydumperVersionTest extends TestCase
{
    public function testParsesTheVersionMyLoaderReports(): void
    {
        $this->assertSame(
            '1.0.5',
            MydumperVersion::parse('myloader v1.0.5-1, built against MySQL 8.4.11 with SSL support')
        );
    }

    public function testParsesTheVersionTheOldBinaryReports(): void
    {
        $this->assertSame(
            '0.19.3',
            MydumperVersion::parse('mydumper v0.19.3-3, built against MariaDB 11.8.3 with SSL support')
        );
    }

    /**
     * The trailing "-1" is the package revision, not part of the upstream version. Treating it as a
     * fourth component would make version_compare read "1.0.5-1" as a pre-release of 1.0.5 and reject
     * the very build we require.
     */
    public function testIgnoresThePackageRevision(): void
    {
        $version = MydumperVersion::parse('myloader v1.0.5-1, built against MySQL 8.4.11');

        $this->assertTrue(MydumperVersion::isSupported($version));
    }

    public function testReturnsNullWhenNoVersionIsPresent(): void
    {
        $this->assertNull(MydumperVersion::parse('myloader: command not found'));
    }

    public function testAcceptsTheFloorAndAnythingAboveIt(): void
    {
        $this->assertTrue(MydumperVersion::isSupported('1.0.5'));
        $this->assertTrue(MydumperVersion::isSupported('1.0.6'));
        $this->assertTrue(MydumperVersion::isSupported('1.1.0'));
        $this->assertTrue(MydumperVersion::isSupported('2.0.0'));
    }

    /**
     * 0.21 sorts below 1.0.5 numerically, but a plain string comparison would put "0.21" above "1.0".
     */
    public function testRejectsEveryVersionBelowTheFloor(): void
    {
        $this->assertFalse(MydumperVersion::isSupported('1.0.4'));
        $this->assertFalse(MydumperVersion::isSupported('0.19.3'));
        $this->assertFalse(MydumperVersion::isSupported('0.21.3'));
    }
}
