[5.0.0](https://github.com/conductorphp/conductor-mysql-database-support/compare/4.2.0...5.0.0) (2026-09-01)


<!--- CHANGELOG SPLIT MARKER -->

[4.2.0](https://github.com/conductorphp/conductor-mysql-database-support/compare/4.1.1...4.2.0) (2026-09-01)

### Features
* sql_mode out of a mydumper dump before myloader runs (CTAP-1571) ([b0599b1](https://github.com/conductorphp/conductor-mysql-database-support/commit/b0599b1f20cb46f31657c1590f49ef2a31eb792d))

<!--- CHANGELOG SPLIT MARKER -->

[4.1.1](https://github.com/conductorphp/conductor-mysql-database-support/compare/4.1.0...4.1.1) (2026-08-11)

### Bug Fixes
* to phpunit 13 (CTAP-1226) ([3d233eb](https://github.com/conductorphp/conductor-mysql-database-support/commit/3d233eb3617708ecd1a79cdf0a89e4c5521b327f))

<!--- CHANGELOG SPLIT MARKER -->

[4.1.0](https://github.com/conductorphp/conductor-mysql-database-support/compare/4.0.0...4.1.0) (2026-08-10)

### Features
* PHP 8.4.1+ (CTAP-1224) ([b3cb154](https://github.com/conductorphp/conductor-mysql-database-support/commit/b3cb154ea354251c47e595712d94b25801959b2d))

<!--- CHANGELOG SPLIT MARKER -->

[3.0.1](https://github.com/conductorphp/conductor-mysql-database-support/compare/3.0.0...3.0.1) (2026-06-25)

### Bug Fixes
* 8.2-8.5 support ([e5047ae](https://github.com/conductorphp/conductor-mysql-database-support/commit/e5047ae686f5b2fd044acb554c2003dc4112a6a7))

<!--- CHANGELOG SPLIT MARKER -->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

### Added

- Added support for PHP 8.1 and 8.2

### Removed

- Removed support for PHP 8.0 and below

## [1.1.0] - Unreleased

### Added

- Added support for PHP 8.0 and 8.1

## [1.0.1] - 2021-04-09

### Fixed

- Fixed replace of definers in all export plugins.
- Updated to replace with CURRENT_USER instead of removing entirely.
- Updated MyDumper export plugin to also replace definers in views.

## [1.0.0] - 2021-01-21

### Added

- Added support for MySql databases.