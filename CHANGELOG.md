# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased
### Added
- Added support for PHP 8.1
- Upgraded all dependencies

## Removed
- Removed support for PHP 8.0 and below

## [1.0.1] - 2021-04-09
### Fixed
- Fixed replace of definers in all export plugins.
  - Updated to replace with CURRENT_USER instead of removing entirely.
  - Updated MyDumper export plugin to also replace definers in views.

## [1.0.0] - 2021-01-21
### Added
- Added support for MySql databases.
