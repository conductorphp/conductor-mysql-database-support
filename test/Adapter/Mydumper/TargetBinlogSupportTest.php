<?php

namespace ConductorMySqlSupportTest\Adapter\Mydumper;

use ConductorMySqlSupport\Adapter\Mydumper\TargetBinlogSupport;
use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;

class TargetBinlogSupportTest extends TestCase
{
    public function testTheRestoreCanBeKeptOutOfTheBinaryLogWhenTheServerAcceptsTheSet(): void
    {
        $connection = $this->createMock(PDO::class);
        $connection->expects($this->once())
            ->method('exec')
            ->with('SET SESSION SQL_LOG_BIN = 0')
            ->willReturn(0);

        $this->assertTrue($this->createSupport($connection)->canDisableBinlog());
    }

    /**
     * A managed database's app user holds ALL PRIVILEGES on its own database only, and the SET needs
     * SUPER, SYSTEM_VARIABLES_ADMIN or SESSION_VARIABLES_ADMIN (CTAP-2267).
     */
    public function testTheRestoreCannotBeKeptOutOfTheBinaryLogWhenTheServerRefusesTheSet(): void
    {
        $connection = $this->createMock(PDO::class);
        $connection->expects($this->once())
            ->method('exec')
            ->willThrowException(new PDOException(
                'SQLSTATE[42000]: Syntax error or access violation: 1227 Access denied; you need (at least one of) '
                . 'the SUPER, SYSTEM_VARIABLES_ADMIN or SESSION_VARIABLES_ADMIN privilege(s) for this operation'
            ));

        $this->assertFalse($this->createSupport($connection)->canDisableBinlog());
    }

    private function createSupport(PDO $connection): TargetBinlogSupport
    {
        return new TargetBinlogSupport('username', 'password', connection: $connection);
    }
}
