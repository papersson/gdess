from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service import catalog
from databricks.sdk.service import iam
from .client import UnityCatalogClient

class UnityCatalog:
    def __init__(self, workspace_client, account_client):
        self.client = UnityCatalogClient(workspace_client, account_client)

    def create_catalog(self, catalog_name):
        """ Create a new catalog and associated groups with default privileges. """
        catalog_info = self.client.create_catalog(catalog_name)
        self.client.create_default_groups(catalog_name)
        self.client.assign_default_permissions(catalog_name)
        print(f"Created catalog '{catalog_name}' and default security groups.")
        return catalog_info

    def delete_catalog(self, catalog_name):
        """ Delete a catalog and its associated groups. """
        # Delete the catalog first to ensure no dependencies prevent group deletion
        self.workspace_client.catalogs.delete(name=catalog_name, force=True)
        print(f"Catalog deleted: {catalog_name}")

        # Fetch and delete groups that start with the catalog_name
        filter_expression = f"displayName sw '{catalog_name}_'"  # SCIM filter for "starts with"
        groups = self.account_client.groups.list(filter=filter_expression)
        for group in groups:
            self.account_client.groups.delete(group.id)
            print(f"Group deleted: {group.display_name} with ID: {group.id}")

    from typing import Optional

    def _find_user_id(self, user_display_name: str) -> Optional[str]:
        """Retrieve user ID based on the user's display name."""
        filter_expression = f"userName eq '{user_display_name}'"
        users = list(self.account_client.users.list(filter=filter_expression))
        if not users:
            raise ValueError(f"User with display name {user_display_name} not found.")
        return users[0].id

    def _find_group_id(self, catalog_name: str, access_type: str) -> Optional[str]:
        """Fetch the group ID using the catalog name and access type."""
        group_display_name = f"{catalog_name}_{access_type}"
        filter_expression = f"displayName eq '{group_display_name}'"
        groups = list(self.account_client.groups.list(filter=filter_expression))
        group = next((group for group in groups if group.display_name == group_display_name), None)
        if not group:
            raise ValueError(f"Group {group_display_name} not found.")
        return group.id

    def grant_access(self, catalog_name: str, access_type: str, user_display_name: str):
        """Grant access to a user based on access type and user's display name."""
        user_id = self._find_user_id(user_display_name)
        group_id = self._find_group_id(catalog_name, access_type)
        operations = [iam.Patch(op=iam.PatchOp.ADD, value={"members": [{"value": user_id}]})]
        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
        self.account_client.groups.patch(id=group_id, operations=operations, schemas=schemas)
        print(f"Granted '{access_type}' access to {user_display_name} for catalog '{catalog_name}'.")

    def revoke_access(self, catalog_name, access_type, user_display_name):
        """ Revoke access from a user based on access type and user's display name by first retrieving the user ID. """
        # Retrieve user ID based on the user's display name using SCIM filter
        filter_expression = f"userName eq '{user_display_name}'"
        users = list(self.account_client.users.list(filter=filter_expression))
        if not users:
            print(f"Error: User with display name {user_display_name} not found.")
            return

        user_id = users[0].id

        # Fetch the group ID using the display name
        group_display_name = f"{catalog_name}_{access_type}"
        filter_expression = f"displayName eq '{group_display_name}'"
        groups = list(self.account_client.groups.list(filter=filter_expression))
        group_id = next((group.id for group in groups if group.display_name == group_display_name), None)
        
        if group_id:
            # Remove the user from the group using the patch method
            operations = [iam.Patch(
                op=iam.PatchOp.REMOVE,
                path="members",  # Specify the path to the attribute being modified
                value={"value": user_id}
            )]
            schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
            self.account_client.groups.patch(id=group_id, operations=operations, schemas=schemas)
            print(f"Access revoked: {user_display_name} removed from {group_display_name}")
        else:
            print(f"Error: Group {group_display_name} not found.")


# import os
# from dotenv import load_dotenv
# from databricks.sdk import WorkspaceClient, AccountClient

# # Load environment variables
# load_dotenv()

# # Use environment variables
# DATABRICKS_HOST_WORKSPACE = os.getenv("DATABRICKS_HOST_WORKSPACE")
# CLIENT_ID = os.getenv("CLIENT_ID")
# CLIENT_SECRET = os.getenv("CLIENT_SECRET")
# DATABRICKS_HOST_ACCOUNT = os.getenv("DATABRICKS_HOST_ACCOUNT")
# ACCOUNT_ID = os.getenv("ACCOUNT_ID")
# # Instantiate a UnityCatalog object
# workspace_client = WorkspaceClient(host=DATABRICKS_HOST_WORKSPACE, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
# account_client = AccountClient(host=DATABRICKS_HOST_ACCOUNT, account_id=ACCOUNT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
# unity_catalog_instance = UnityCatalog(workspace_client, account_client)
# # Create a temporary catalog for demonstration
# temp_catalog_name = "test_access"
# unity_catalog_instance.create_catalog(temp_catalog_name)
# # # Example usage of the grant_access method
# # # Assuming 'user_principal_id' is the ID of the user to whom access is being granted
# user_principal_id = 'toni.krmek@falck.com'
# access_type = "read"  # Can be 'read', 'readwrite', or 'writemetadata'
# try:
#     # unity_catalog_instance.revoke_access(temp_catalog_name, access_type, user_principal_id)
#     unity_catalog_instance.grant_access(temp_catalog_name, access_type, user_principal_id)
#     print(f"Access type '{access_type}' granted to user with name {user_principal_id} for catalog '{temp_catalog_name}'.")
# except Exception as e:
#     print(f"Failed to grant access: {e}")
# # Cleanup: Delete the temporary catalog
# unity_catalog_instance.delete_catalog(temp_catalog_name)

