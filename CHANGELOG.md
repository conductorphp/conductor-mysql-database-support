Robofirm DevOps Tool: MySQL Database Support
============================================

# 0.2.0 (Unreleased)
- Updated PHP version requirement to 7.1
- Cleaned up DatabaseAdapter code
- Removed unused FilesystemTransfer classes
- Updated to use named adapters rather than formats with connections
- Combined DatabaseImportAdapterInterface and DatabaseExportAdapterInterface into DatabaseImportExportAdapterInterface
- Refactored all adapters to use DatabaseImportExportAdapterInterface; Kept import/export is separate "plugin" classes
  since each operation is sufficiently complex to warrant a dedicated class

# 0.1.1
- Removed DI config for \DevopsToolCore\Database\DatabaseMetadataProviderInterface
- Added DatabaseAdapterFactory
- Merged DatabaseMetadataProvider into DatabaseAdapter
- Fixed quoating of db name in dropTableIfExists and added exception on error

# 0.1.0
- Initial build
