Conductor MySQL Database Support Documentation
==============================================

This module adds support for MySQL databases in
[Conductor](https://github.com/conductorphp/conductor-core).

## Installation

```bash
composer require conductor/mysql-database-support
``` 

### Requirements

The mydumper adapter requires **mydumper/myloader 1.0.5 or newer** on the host that runs the
import or export. mydumper 1.0 renamed the loader options this adapter drives (`--overwrite-tables`
became `-o, --drop-table`), and there is no spelling both the 0.x and 1.0 lines accept, so the
version is asserted up front rather than shimmed. An older binary fails with a message naming the
version it found and the one it needs.

Note that snapshots are only forward compatible: a dump written by 0.x restores under 1.0, but a
dump written by 1.0 cannot be restored by 0.x myloader. Upgrade the loader before you start taking
snapshots with it.

## Restoring a dump onto a different engine

The mydumper adapter removes `sql_mode` values the target server does not recognize from an extracted
dump before handing it to `myloader`.

mydumper records the **source** server's `sql_mode`, both in the `metadata` file's
`[myloader_session_variables]` group (which myloader applies to every connection it opens) and as a
`SET SQL_MODE=...` header in every `.sql` file. A MariaDB source contributes `NO_AUTO_CREATE_USER`,
which MySQL 8 removed, so every myloader thread dies at connection setup with
`ERROR 1231: Variable 'sql_mode' can't be set to the value of 'NO_AUTO_CREATE_USER'` before a single
row is read. The `/*!40101 ... */` gate the header sits inside does not help — version-gated comments
run on servers at or *above* that version, so MySQL 8 executes the statement and rejects the mode.

The adapter asks the target server which of the dump's modes it will accept and strips only those it
refuses, logging what it removed. `sql_mode` is a session setting the target no longer knows, so
dropping it changes nothing about the data.

Two things it deliberately does not do:

* **It does not rewrite schema DDL.** A `DEFAULT` on a `longtext` column or an over-long `UNIQUE KEY`
  that a newer server rejects is real schema, and quietly editing a production schema on the way in is
  a worse problem than the one it solves. Such a dump is refused before the restore starts — see
  below.
* **It changes nothing when the target accepts every mode.** A restore onto the same engine the dump
  came from leaves the dump byte for byte as extracted.

A dump is a photograph of the schema at capture time, so a schema fix deployed after a snapshot was
taken does nothing for that snapshot. To restore onto a different engine, take the snapshot from a
source that already carries the portability fixes.

## A restore that cannot succeed does not start

myloader drops and recreates each table as it reaches it, so a dump the target will not take does not
fail cleanly — it fails partway, having already replaced whatever was there. A pre-fix snapshot
measured against a MySQL 8 target got 71 tables in before it died, destroying 71 tables' worth of a
working database for a restore that was never going to finish.

So the import rehearses the dump's table definitions first. It creates a scratch database on the
target (`conductor_restore_preflight_<random>`), creates every `*-schema.sql` definition in it under
the same session settings myloader will use, drops it, and refuses the restore if the server rejected
anything — naming the table, the file, and the server's own reason:

```
The target server will not accept 1 of the 3 table definition(s) in this dump:
  `permissions_holder` (mydb.permissions_holder-schema.sql): SQLSTATE[42000]: Syntax error or access
  violation: 1101 BLOB, TEXT, GEOMETRY or JSON column 'permissions' can't have a default value
The restore was stopped before it changed anything...
```

Nothing about the real database changes on that path. On failure the extracted dump and myloader's
output are left on disk, and their locations logged, so there is something to look at.

Only table definitions are rehearsed. Views, triggers and routines reference objects a scratch
database does not have, so trying them there would fail definitions that restore perfectly well.

The check needs permission to create a database. Where it does not have it — or cannot reach the
server, or breaks partway — it logs a warning and lets the restore proceed unchecked, rather than
blocking a restore that may well work.

Restoring into a database that already holds tables is also announced before myloader runs. The
deploy plan drops the database first and asks before it does; `conductor database:import` overlays
into whatever is there and, until now, said nothing at all.

## An export that produced nothing is not a successful export

`mydumper` exiting 0 is not proof that it wrote anything, and the shell adapter only sees the exit
status. So the export checks its own artifact before reporting it: the archive exists, is not empty,
and lists a `metadata` entry (which myloader requires). Listing reads the whole archive, which is
also the only thing that proves the gzip stream runs to the end — an export truncated by a full disk
tars and exits 0.

The export also runs in the working directory it creates under the target path. Every path in the
command it builds is relative, so left to the process's own working directory the dump and the
archive landed wherever conductor happened to be started from, while the returned path claimed
otherwise. On success the working directory is removed; on failure it is left in place, and named in
the error, so there is something to diagnose.
