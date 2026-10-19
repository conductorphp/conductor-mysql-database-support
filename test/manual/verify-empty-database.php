<?php

/**
 * CTAP-2756 — manual verification for DatabaseAdapter::emptyDatabase() against a LIVE database.
 *
 * Not a PHPUnit test, and deliberately not named *Test.php so no suite collects it: it needs a
 * reachable MySQL/MariaDB with rights to create a database and a user, which CI here does not have.
 * Whether the server accepts the drops, in which order, with which rights, is the whole point, so a
 * mocked connection would assert nothing.
 *
 * It builds a database as root, then empties it as an account that has no CREATE or DROP on the
 * database itself (only DROP on each table and view, ALTER ROUTINE and EVENT), and checks:
 *   - tables with foreign keys in both directions (a cycle) and a trigger
 *   - a view, and a view over a table that has since been dropped
 *   - a table whose name needs quoting
 *   - a procedure, a function and an event
 *   - the database, its character set and the account's grants are still there afterwards
 *   - foreign key checks are back on for the adapter's session
 *   - emptying an empty or missing database does nothing
 *   - a view defined by root is refused before any table is dropped
 *
 * Usage — from a container that can reach the database, with an autoloader that has this package:
 *
 *   php test/manual/verify-empty-database.php [host] [root user] [root password]
 *
 * Defaults: host `127.0.0.1`, user `root`, password `root`. Exits 0 when every assertion passes,
 * 1 otherwise. It creates and DROPS the database `ctap2756_empty_test` and the user
 * `ctap2756_restore` on the target server.
 *
 * Verified passing on PHP 8.4.25 and 8.5.10 against MySQL 8.4.11.
 */

declare(strict_types=1);

use ConductorMySqlSupport\Adapter\DatabaseAdapter;

$autoloaders = array_filter([
    getenv('CONDUCTOR_AUTOLOAD') ?: null,
    __DIR__ . '/../../vendor/autoload.php',                                 // package installed standalone
    __DIR__ . '/../../../../autoload.php',                                  // installed as a dependency
    __DIR__ . '/../../../../tools/lib-tests/vendor-84/autoload.php',        // conductor monorepo, PHP 8.4
    __DIR__ . '/../../../../tools/lib-tests/vendor-85/autoload.php',        // conductor monorepo, PHP 8.5
]);
foreach ($autoloaders as $autoloader) {
    if (file_exists($autoloader)) {
        require $autoloader;
        break;
    }
}
if (! method_exists(DatabaseAdapter::class, 'emptyDatabase')) {
    fwrite(STDERR, 'No autoloader found that provides ' . DatabaseAdapter::class . "::emptyDatabase().\n"
        . "Point CONDUCTOR_AUTOLOAD at one:\n"
        . '  CONDUCTOR_AUTOLOAD=/path/to/vendor/autoload.php php ' . $argv[0] . " <host> [user] [password]\n");
    exit(2);
}

$host         = $argv[1] ?? '127.0.0.1';
$rootUser     = $argv[2] ?? 'root';
$rootPassword = $argv[3] ?? 'root';
$db           = 'ctap2756_empty_test';
$user         = 'ctap2756_restore';
$password     = bin2hex(random_bytes(8));

$pdo = new PDO("mysql:host=$host;port=3306", $rootUser, $rootPassword, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

echo "server: " . $pdo->getAttribute(PDO::ATTR_SERVER_VERSION) . "  (host $host)\n\n";

$failures = 0;
$check = static function (bool $ok, string $what) use (&$failures): void {
    echo ($ok ? 'PASS ' : 'FAIL ') . $what . "\n";
    if (! $ok) {
        $failures++;
    }
};
$count = static fn (string $sql): int => (int) $pdo->query($sql)->fetchColumn();

$pdo->exec("DROP DATABASE IF EXISTS `$db`");
$pdo->exec("DROP USER IF EXISTS '$user'@'%'");
// Not the server default, so a recreated database would show
$pdo->exec("CREATE DATABASE `$db` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci");
$pdo->exec("USE `$db`");
$pdo->exec("CREATE USER '$user'@'%' IDENTIFIED BY '$password'");
// Views, routines, triggers and events are defined by the account that restores, as they are when a
// restore or a migration created them. One defined by a SYSTEM_USER account such as root cannot be
// dropped without SYSTEM_USER on MySQL 8.
$definer = "DEFINER = '$user'@'%'";

$pdo->exec('CREATE TABLE parent (id INT PRIMARY KEY, favorite_child_id INT NULL) ENGINE=InnoDB');
$pdo->exec('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT NOT NULL,
    CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES parent (id)) ENGINE=InnoDB');
$pdo->exec('ALTER TABLE parent ADD CONSTRAINT parent_child FOREIGN KEY (favorite_child_id) REFERENCES child (id)');
$pdo->exec('INSERT INTO parent VALUES (1, NULL)');
$pdo->exec('INSERT INTO child VALUES (10, 1)');
$pdo->exec('UPDATE parent SET favorite_child_id = 10');
$pdo->exec("CREATE $definer TRIGGER child_touch BEFORE UPDATE ON child FOR EACH ROW SET NEW.id = NEW.id");
$pdo->exec('CREATE TABLE `odd``name-x` (id INT PRIMARY KEY)');
$pdo->exec('CREATE TABLE gone (id INT PRIMARY KEY)');
$pdo->exec("CREATE $definer VIEW v_live AS SELECT id FROM parent");
$pdo->exec("CREATE $definer VIEW v_broken AS SELECT id FROM gone");
$pdo->exec('DROP TABLE gone');
$pdo->exec("CREATE $definer PROCEDURE p_noop() BEGIN END");
$pdo->exec("CREATE $definer FUNCTION f_one() RETURNS INT DETERMINISTIC NO SQL RETURN 1");
$pdo->exec("CREATE $definer EVENT e_noop ON SCHEDULE EVERY 1 DAY DISABLE DO SET @noop = 1");

// No CREATE or DROP at the database level: the account cannot drop or create the database
$pdo->exec("GRANT ALTER ROUTINE, EVENT ON `$db`.* TO '$user'@'%'");
foreach (['parent', 'child', 'odd``name-x', 'v_live', 'v_broken'] as $table) {
    $pdo->exec("GRANT SELECT, DROP ON `$db`.`$table` TO '$user'@'%'");
}

$restricted = new PDO("mysql:host=$host;port=3306", $user, $password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
foreach (["DROP DATABASE `$db`", "CREATE DATABASE `{$db}_other`"] as $statement) {
    try {
        $restricted->exec($statement);
        $check(false, "the account is refused \"$statement\"");
    } catch (PDOException) {
        $check(true, "the account is refused \"$statement\"");
    }
}

$adapter = new DatabaseAdapter($user, $password, $host);
try {
    $adapter->emptyDatabase($db);
    $check(true, 'emptyDatabase() succeeds as the restricted account');
} catch (Throwable $e) {
    $check(false, 'emptyDatabase() succeeds as the restricted account: ' . $e->getMessage());
}

$where = static fn (string $column): string => "WHERE $column = '$db'";
$check(0 === $count("SELECT COUNT(*) FROM information_schema.TABLES {$where('TABLE_SCHEMA')}"), 'no tables or views left');
$check(0 === $count("SELECT COUNT(*) FROM information_schema.ROUTINES {$where('ROUTINE_SCHEMA')}"), 'no routines left');
$check(0 === $count("SELECT COUNT(*) FROM information_schema.EVENTS {$where('EVENT_SCHEMA')}"), 'no events left');
$check(0 === $count("SELECT COUNT(*) FROM information_schema.TRIGGERS {$where('TRIGGER_SCHEMA')}"), 'no triggers left');
$check(
    'latin1' === $pdo->query("SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA {$where('SCHEMA_NAME')}")->fetchColumn(),
    'the database is still there with its own character set'
);
$grants = implode("\n", $pdo->query("SHOW GRANTS FOR '$user'@'%'")->fetchAll(PDO::FETCH_COLUMN));
$check(str_contains($grants, "ON `$db`.*"), "the account's grants on the database survive");
$check(
    '1' === (string) $adapter->fetchAll('SELECT @@FOREIGN_KEY_CHECKS AS fk', $db)[0]['fk'],
    "foreign key checks are back on for the adapter's session"
);

try {
    $adapter->emptyDatabase($db);
    $check(true, 'emptying an empty database does nothing');
} catch (Throwable $e) {
    $check(false, 'emptying an empty database does nothing: ' . $e->getMessage());
}
try {
    (new DatabaseAdapter($rootUser, $rootPassword, $host))->emptyDatabase('ctap2756_does_not_exist');
    $check(true, 'emptying a missing database does nothing');
} catch (Throwable $e) {
    $check(false, 'emptying a missing database does nothing: ' . $e->getMessage());
}

// A view whose definer has SYSTEM_USER is refused, and the refusal comes before any table is dropped
$pdo->exec('CREATE TABLE kept (id INT PRIMARY KEY)');
$pdo->exec("CREATE DEFINER = 'root'@'localhost' VIEW v_root AS SELECT id FROM kept");
$pdo->exec("GRANT SELECT, DROP ON `$db`.`kept` TO '$user'@'%'");
$pdo->exec("GRANT SELECT, DROP ON `$db`.`v_root` TO '$user'@'%'");
try {
    $adapter->emptyDatabase($db);
    $check(false, 'a view defined by root is refused');
} catch (Throwable $e) {
    $check(str_contains($e->getMessage(), 'v_root'), 'a view defined by root is refused, and the error names it');
}
$check(1 === $count("SELECT COUNT(*) FROM information_schema.TABLES {$where('TABLE_SCHEMA')} AND TABLE_NAME = 'kept'"), 'no table is dropped before the refusal');

$pdo->exec("DROP DATABASE IF EXISTS `$db`");
$pdo->exec("DROP USER IF EXISTS '$user'@'%'");

echo "\n" . ($failures ? "$failures FAILED\n" : "ALL PASSED\n");
exit($failures ? 1 : 0);
