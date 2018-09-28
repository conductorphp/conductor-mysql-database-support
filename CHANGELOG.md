Conductor: MySQL Database Support
============================================

# 0.9.2
- Fixed license per https://spdx.org/licenses/

# 0.9.1
- Updated conductor/core require to ~0.9.2

# 0.9.0
- Tagged for initial consistency with other modules

# 0.2.0 (Unreleased)
- Renamed to Conductor
- Updated PHP version requirement to 7.1
- Cleaned up DatabaseAdapter code
- Removed unused FilesystemTransfer classes
- Updated to use named adapters rather than formats with connections
- Combined DatabaseImportAdapterInterface and DatabaseExportAdapterInterface into DatabaseImportExportAdapterInterface
- Refactored all adapters to use DatabaseImportExportAdapterInterface; Kept import/export is separate "plugin" classes
  since each operation is sufficiently complex to warrant a dedicated class

# 0.1.1
- Removed DI config for \ConductorCore\Database\DatabaseMetadataProviderInterface
- Added DatabaseAdapterFactory
- Merged DatabaseMetadataProvider into DatabaseAdapter
- Fixed quoating of db name in dropTableIfExists and added exception on error

# 0.1.0
- Initial build
