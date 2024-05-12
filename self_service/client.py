from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service import catalog

class UnityCatalogClient:
    def __init__(self, workspace_client, account_client):
        self.workspace_client = workspace_client
        self.account_client = account_client

    def create_catalog(self, catalog_name):
        """Create a catalog using the workspace client."""
        return self.workspace_client.catalogs.create(name=catalog_name)

    def create_group(self, display_name):
        """Create a group using the account client."""
        return self.account_client.groups.create(display_name=display_name)

    def delete_catalog(self, catalog_name, force=True):
        """Delete a catalog using the workspace client."""
        return self.workspace_client.catalogs.delete(name=catalog_name, force=force)

    def list_groups(self, filter_expression):
        """List groups using the account client with a filter."""
        return self.account_client.groups.list(filter=filter_expression)

    def delete_group(self, group_id):
        """Delete a group using the account client."""
        return self.account_client.groups.delete(group_id)

    def update_grants(self, securable_type, full_name, changes):
        """Update grants for a catalog using the workspace client."""
        return self.workspace_client.grants.update(securable_type=securable_type, full_name=full_name, changes=changes)
    
    def create_default_groups(self, catalog_name):
        """Create default groups for a catalog."""
        group_suffixes = ["read", "readwrite", "writemetadata"]
        groups_info = {}
        for suffix in group_suffixes:
            display_name = f"{catalog_name}_{suffix}"
            group_info = self.create_group(display_name)
            groups_info[display_name] = group_info.id
        return groups_info
    
    def assign_default_permissions(self, catalog_name):
        """Assign default permissions to groups associated with a catalog."""
        changes = [
            catalog.PermissionsChange(add=[catalog.Privilege.USE_CATALOG, catalog.Privilege.USE_SCHEMA, catalog.Privilege.SELECT], principal=f"{catalog_name}_read"),
            catalog.PermissionsChange(add=[catalog.Privilege.ALL_PRIVILEGES], principal=f"{catalog_name}_readwrite"),
            catalog.PermissionsChange(add=[catalog.Privilege.APPLY_TAG], principal=f"{catalog_name}_writemetadata")
        ]
        self.update_grants(catalog.SecurableType.CATALOG, catalog_name, changes)