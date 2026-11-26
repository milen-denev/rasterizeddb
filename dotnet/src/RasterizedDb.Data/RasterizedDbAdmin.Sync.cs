// Generated from the asynchronous calls of RasterizedDbAdmin.cs: one synchronous twin for each.

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace RasterizedDb.Data;

/// <content>The synchronous calls: each is its <c>Async</c> twin, sent with blocking I/O on the calling thread.</content>
public readonly partial struct RasterizedDbAdmin
{
    public List<string> AddStore(RasterizedDbStore store) =>
        Blocking.AddStoreAsync(store).GetAwaiter().GetResult();

    public List<string> AlterStore(string name, IEnumerable<KeyValuePair<string, string>> settings) =>
        Blocking.AlterStoreAsync(name, settings).GetAwaiter().GetResult();

    public List<string> RotateStoreCredentials(string name, string accessKeyId, string secretAccessKey) =>
        Blocking.RotateStoreCredentialsAsync(name, accessKeyId, secretAccessKey).GetAwaiter().GetResult();

    public List<string> DropStore(string name) =>
        Blocking.DropStoreAsync(name).GetAwaiter().GetResult();

    public bool DropStoreIfExists(string name) =>
        Blocking.DropStoreIfExistsAsync(name).GetAwaiter().GetResult();

    public List<RasterizedDbStoreInfo> Stores() =>
        Blocking.StoresAsync().GetAwaiter().GetResult();

    public RasterizedDbStoreInfo? Store(string name) =>
        Blocking.StoreAsync(name).GetAwaiter().GetResult();

    public List<string> PlaceTable(string table, RasterizedDbPlacement placement) =>
        Blocking.PlaceTableAsync(table, placement).GetAwaiter().GetResult();

    public List<string> MoveTable(string table, RasterizedDbPlacement placement) =>
        Blocking.MoveTableAsync(table, placement).GetAwaiter().GetResult();

    public List<RasterizedDbLocationInfo> StorageLocations() =>
        Blocking.StorageLocationsAsync().GetAwaiter().GetResult();

    public List<RasterizedDbMetric> StorageStats() =>
        Blocking.StorageStatsAsync().GetAwaiter().GetResult();

    public void CreateIndex(RasterizedDbIndex index) =>
        Blocking.CreateIndexAsync(index).GetAwaiter().GetResult();

    public void DropIndex(string name, bool ifExists = false) =>
        Blocking.DropIndexAsync(name, ifExists).GetAwaiter().GetResult();

    public string IndexCheckpoint(string table) =>
        Blocking.IndexCheckpointAsync(table).GetAwaiter().GetResult();

    public void PartitionByHash(string table, IReadOnlyList<string> columns, int? partitions = null) =>
        Blocking.PartitionByHashAsync(table, columns, partitions).GetAwaiter().GetResult();

    public List<RasterizedDbPartitionInfo> Partitions(string table) =>
        Blocking.PartitionsAsync(table).GetAwaiter().GetResult();

    public long Vacuum(string? table = null, bool full = false) =>
        Blocking.VacuumAsync(table, full).GetAwaiter().GetResult();

    public List<RasterizedDbTableInfo> Tables() =>
        Blocking.TablesAsync().GetAwaiter().GetResult();

    public List<RasterizedDbColumnInfo> Columns(string table) =>
        Blocking.ColumnsAsync(table).GetAwaiter().GetResult();

    public List<RasterizedDbConfigEntry> Config() =>
        Blocking.ConfigAsync().GetAwaiter().GetResult();

    public string SetConfig(string key, string value) =>
        Blocking.SetConfigAsync(key, value).GetAwaiter().GetResult();

    public List<RasterizedDbMetric> DatabaseStats() =>
        Blocking.DatabaseStatsAsync().GetAwaiter().GetResult();

    public List<RasterizedDbMetric> TableStats(string table) =>
        Blocking.TableStatsAsync(table).GetAwaiter().GetResult();

    public void CreateRole(RasterizedDbRole role) =>
        Blocking.CreateRoleAsync(role).GetAwaiter().GetResult();

    public void AlterRole(RasterizedDbRole role) =>
        Blocking.AlterRoleAsync(role).GetAwaiter().GetResult();

    public void RenameRole(string from, string to) =>
        Blocking.RenameRoleAsync(from, to).GetAwaiter().GetResult();

    public void SetRoleSetting(string name, string setting, string? value) =>
        Blocking.SetRoleSettingAsync(name, setting, value).GetAwaiter().GetResult();

    public void GrantRole(string role, string member, bool withAdminOption = false) =>
        Blocking.GrantRoleAsync(role, member, withAdminOption).GetAwaiter().GetResult();

    public void RevokeRole(string role, string member) =>
        Blocking.RevokeRoleAsync(role, member).GetAwaiter().GetResult();

    public void Grant(RasterizedDbGrant grant) =>
        Blocking.GrantAsync(grant).GetAwaiter().GetResult();

    public void Revoke(RasterizedDbGrant grant) =>
        Blocking.RevokeAsync(grant).GetAwaiter().GetResult();

    public void ReassignOwned(string from, string to) =>
        Blocking.ReassignOwnedAsync(from, to).GetAwaiter().GetResult();

    public List<RasterizedDbRoleInfo> Roles() =>
        Blocking.RolesAsync().GetAwaiter().GetResult();

    public List<RasterizedDbGrantInfo> TableGrants(string table) =>
        Blocking.TableGrantsAsync(table).GetAwaiter().GetResult();

    public void SetRolePassword(string name, string? password) =>
        Blocking.SetRolePasswordAsync(name, password).GetAwaiter().GetResult();

    public void DropRole(string name, bool ifExists = false) =>
        Blocking.DropRoleAsync(name, ifExists).GetAwaiter().GetResult();

    public long BackupDatabase() =>
        Blocking.BackupDatabaseAsync().GetAwaiter().GetResult();

    public long BackupWal() =>
        Blocking.BackupWalAsync().GetAwaiter().GetResult();

    public long BackupPrune() =>
        Blocking.BackupPruneAsync().GetAwaiter().GetResult();

    public List<RasterizedDbBackupInfo> Backups() =>
        Blocking.BackupsAsync().GetAwaiter().GetResult();

    public List<RasterizedDbMetric> BackupStatus() =>
        Blocking.BackupStatusAsync().GetAwaiter().GetResult();

    public RasterizedDbRestoreReport Restore(RasterizedDbRestoreTarget target, string toDirectory) =>
        Blocking.RestoreAsync(target, toDirectory).GetAwaiter().GetResult();

    public List<RasterizedDbMetric> ReplicationStats() =>
        Blocking.ReplicationStatsAsync().GetAwaiter().GetResult();

    public List<Dictionary<string, object?>> ReplicationPeers() =>
        Blocking.ReplicationPeersAsync().GetAwaiter().GetResult();

    public List<string> Replication(string statement) =>
        Blocking.ReplicationAsync(statement).GetAwaiter().GetResult();
}
