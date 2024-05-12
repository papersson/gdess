from typing import Optional
from databricks.sdk.service import catalog
from databricks.sdk.service import iam

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
        self.workspace_client.grants.update(securable_type=catalog.SecurableType.CATALOG, full_name=catalog_name, changes=changes)

    def delete_catalog_and_groups(self, catalog_name):
            """Delete a catalog and its associated groups."""
            # Delete the catalog first to ensure no dependencies prevent group deletion
            self.workspace_client.catalogs.delete(name=catalog_name, force=True)

            # Fetch and delete groups that start with the catalog_name
            filter_expression = f"displayName sw '{catalog_name}_'"
            groups = self.account_client.groups.list(filter=filter_expression)
            for group in groups:
                self.account_client.groups.delete(group.id)

    def find_user_id(self, user_display_name: str) -> Optional[str]:
        """Retrieve user ID based on the user's display name."""
        filter_expression = f"userName eq '{user_display_name}'"
        users = list(self.account_client.users.list(filter=filter_expression))
        if not users:
            raise ValueError(f"User with display name {user_display_name} not found.")
        return users[0].id

    def find_group_id(self, catalog_name: str, access_type: str) -> Optional[str]:
        """Fetch the group ID using the catalog name and access type."""
        group_display_name = f"{catalog_name}_{access_type}"
        filter_expression = f"displayName eq '{group_display_name}'"
        groups = list(self.account_client.groups.list(filter=filter_expression))
        if not groups:
            raise ValueError(f"Group {group_display_name} not found.")
        return groups[0].id
    
    def grant_access(self, catalog_name: str, access_type: str, user_display_name: str):
        """Grant access to a user based on access type and user's display name."""
        user_id = self.find_user_id(user_display_name)
        group_id = self.find_group_id(catalog_name, access_type)
        operations = [iam.Patch(
            op=iam.PatchOp.ADD,
            path="members",
            value={"value": user_id}
        )]
        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
        self.account_client.groups.patch(id=group_id, operations=operations, schemas=schemas)

    def revoke_access(self, catalog_name: str, access_type: str, user_display_name: str):
        """Revoke access from a user based on access type and user's display name."""
        user_id = self.find_user_id(user_display_name)
        group_id = self.find_group_id(catalog_name, access_type)
        operations = [iam.Patch(
            op=iam.PatchOp.REMOVE,
            path="members",
            value={"value": user_id}
        )]
        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
        self.account_client.groups.patch(id=group_id, operations=operations, schemas=schemas)