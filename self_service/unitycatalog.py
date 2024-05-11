from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service import catalog

class UnityCatalog:
    def __init__(self, workspace_client, account_client):
        self.workspace_client = workspace_client
        self.account_client = account_client

    def create_catalog(self, catalog_name):
        """ Create a new catalog and associated groups with default privileges. """
        # Create catalog
        catalog_info = self.workspace_client.catalogs.create(name=catalog_name)
        print(f"Catalog created: {catalog_info.name}")
        
        # Create groups with names derived from the catalog name
        groups_info = {}
        for group_name_suffix in ["read", "readwrite", "writemetadata"]:
            group_display_name = f"{catalog_name}_{group_name_suffix}"
            group_info = self.account_client.groups.create(display_name=group_display_name)
            groups_info[group_display_name] = group_info.id
            print(f"Group created: {group_display_name} with ID: {group_info.id}")

        # Assign default permissions to groups
        self.workspace_client.grants.update(securable_type=catalog.SecurableType.CATALOG, full_name=catalog_name, changes=[
            catalog.PermissionsChange(add=[catalog.Privilege.USE_CATALOG, catalog.Privilege.USE_SCHEMA, catalog.Privilege.SELECT], principal=f"{catalog_name}_read"),
            catalog.PermissionsChange(add=[catalog.Privilege.ALL_PRIVILEGES], principal=f"{catalog_name}_readwrite"),
            catalog.PermissionsChange(add=[catalog.Privilege.APPLY_TAG], principal=f"{catalog_name}_writemetadata")
        ])
        print("Permissions assigned to groups.")

        return catalog_info

    def delete_catalog(self, catalog_name):
        """ Delete a catalog and its associated groups. """
        # Delete the catalog first to ensure no dependencies prevent group deletion
        self.workspace_client.catalogs.delete(name=catalog_name, force=True)
        print(f"Catalog deleted: {catalog_name}")

        # Fetch and delete groups that start with the catalog_name
        filter_expression = f"displayName sw '{catalog_name}'"  # SCIM filter for "starts with"
        groups = self.account_client.groups.list(filter=filter_expression)
        for group in groups:
            self.account_client.groups.delete(group.id)
            print(f"Group deleted: {group.display_name} with ID: {group.id}")