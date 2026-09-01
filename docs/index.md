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
  a worse problem than the one it solves. Such a dump fails, with myloader's own error naming the
  column or file.
* **It changes nothing when the target accepts every mode.** A restore onto the same engine the dump
  came from leaves the dump byte for byte as extracted.

A dump is a photograph of the schema at capture time, so a schema fix deployed after a snapshot was
taken does nothing for that snapshot. To restore onto a different engine, take the snapshot from a
source that already carries the portability fixes.
