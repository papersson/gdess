from databricks.sdk.service import catalog
import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

class Access:
    def __init__(self) -> None:
        """
        Initialize Access by setting up a workspace client using environment variables. This base class
        configures the client needed to interact with Databricks services.

        The purpose of this class is to provide a simpler and higher-level interface for managing access
        permissions in Unity Catalog. This security model uses three core access types (read, readwrite,
        writemetadata) on every level (catalog, schema, table), and this class orchestrates the low-level
        permission changes needed to achieve the desired access type.
        """
        load_dotenv()
        DATABRICKS_HOST_WORKSPACE: str = os.getenv("DATABRICKS_HOST_WORKSPACE", "")
        CLIENT_ID: str = os.getenv("CLIENT_ID", "")
        CLIENT_SECRET: str = os.getenv("CLIENT_SECRET", "")
        
        if not all([DATABRICKS_HOST_WORKSPACE, CLIENT_ID, CLIENT_SECRET]):
            raise EnvironmentError("Required environment variables for Databricks workspace are not set.")
        
        self.client = WorkspaceClient(host=DATABRICKS_HOST_WORKSPACE, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

    def _update_permissions(self, access_type: str, principal: str, action: str) -> None:
        """
        Abstract method to update permissions based on the specified access type, principal, and
        action. Must be implemented by subclasses to handle specific permission logic.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    def grant(self, access_type: str, principal: str) -> None:
        """
        Grant specified permissions to a principal based on the access type.

        Args:
            access_type (str): The type of access to grant or revoke ('read', 'readwrite', 'writemetadata').
            principal (str): The principal (user or group) to which the permissions are applied.
        """
        self._update_permissions(access_type, principal, 'add')

    def revoke(self, access_type: str, principal: str) -> None:
        """
        Revoke specified permissions from a principal based on the access type.

        Args:
            access_type (str): The type of access to grant or revoke ('read', 'readwrite', 'writemetadata').
            principal (str): The principal (user or group) to which the permissions are applied.
        """
        self._update_permissions(access_type, principal, 'remove')

class CatalogAccess(Access):
    def __init__(self, full_name: str) -> None:
        """Initialize CatalogAccess with specific catalog settings.
        
        Args:
            full_name (str): The full name of the catalog.
        """
        super().__init__()
        self.full_name = full_name

    def _update_permissions(self, access_type: str, principal: str, action: str) -> None:
        """ Update permissions excluvely on the catalog. """
        privileges_mapping = {
            'read': [catalog.Privilege.USE_CATALOG, catalog.Privilege.USE_SCHEMA, catalog.Privilege.SELECT],
            'readwrite': [catalog.Privilege.ALL_PRIVILEGES],
            'writemetadata': [catalog.Privilege.APPLY_TAG]
        }

        if access_type not in privileges_mapping:
            raise ValueError("Unsupported access type")

        changes = catalog.PermissionsChange(**{action: privileges_mapping[access_type]}, principal=principal)
        self.client.grants.update(
            securable_type=catalog.SecurableType.CATALOG,
            full_name=self.full_name,
            changes=[changes]
        )

class SchemaAccess(Access):
    def __init__(self, catalog_name: str, schema_name: str) -> None:
        """Initialize SchemaAccess with specific schema settings.
        
        Args:
            catalog_name (str): The name of the catalog.
            schema_name (str): The name of the schema.
        """
        super().__init__()
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.full_name = f"{catalog_name}.{schema_name}"

    def _update_permissions(self, access_type: str, principal: str, action: str) -> None:
        """ Update permissions on the schema and parent catalog. """
        privileges_mapping = {
            'read': [
                (catalog.SecurableType.CATALOG, [catalog.Privilege.USE_CATALOG]),
                (catalog.SecurableType.SCHEMA, [catalog.Privilege.USE_SCHEMA, catalog.Privilege.SELECT])
            ],
            'readwrite': [
                (catalog.SecurableType.CATALOG, [catalog.Privilege.USE_CATALOG]),
                (catalog.SecurableType.SCHEMA, [catalog.Privilege.ALL_PRIVILEGES])
            ],
            'writemetadata': [
                (catalog.SecurableType.CATALOG, [catalog.Privilege.USE_CATALOG]),
                (catalog.SecurableType.SCHEMA, [catalog.Privilege.APPLY_TAG])
            ]
        }

        if access_type not in privileges_mapping:
            raise ValueError("Unsupported access type")

        for securable_type, privileges in privileges_mapping[access_type]:
            changes = catalog.PermissionsChange(**{action: privileges}, principal=principal)
            full_name = self.catalog_name if securable_type == catalog.SecurableType.CATALOG else f"{self.catalog_name}.{self.schema_name}"
            self.client.grants.update(
                securable_type=securable_type,
                full_name=full_name,
                changes=[changes]
            )

class TableAccess(Access):
    def __init__(self, catalog_name: str, schema_name: str, table_name: str) -> None:
        """Initialize SchemaAccess with specific schema settings.
        
        Args:
            catalog_name (str): The name of the catalog.
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.
        """
        super().__init__()
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_name = f"{catalog_name}.{schema_name}.{table_name}"

    def _update_permissions(self, access_type: str, principal: str, action: str) -> None:
        """ Update permissions on the schema and parent catalog. """
        privileges_mapping = {
            'read': [
                (catalog.SecurableType.CATALOG, [catalog.Privilege.USE_CATALOG]),
                (catalog.SecurableType.SCHEMA, [catalog.Privilege.USE_SCHEMA]),
                (catalog.SecurableType.TABLE, [catalog.Privilege.SELECT])
            ],
            'readwrite': [
                (catalog.SecurableType.CATALOG, [catalog.Privilege.USE_CATALOG]),
                (catalog.SecurableType.SCHEMA, [catalog.Privilege.USE_SCHEMA]),
                (catalog.SecurableType.TABLE, [catalog.Privilege.ALL_PRIVILEGES])
            ],
            'writemetadata': [
                (catalog.SecurableType.CATALOG, [catalog.Privilege.USE_CATALOG]),
                (catalog.SecurableType.SCHEMA, [catalog.Privilege.USE_SCHEMA]),
                (catalog.SecurableType.TABLE, [catalog.Privilege.APPLY_TAG])
            ]
        }

        if access_type not in privileges_mapping:
            raise ValueError("Unsupported access type")

        for securable_type, privileges in privileges_mapping[access_type]:
            changes = catalog.PermissionsChange(**{action: privileges}, principal=principal)
            full_name_map = {
                catalog.SecurableType.TABLE: f"{self.catalog_name}.{self.schema_name}.{self.table_name}",
                catalog.SecurableType.SCHEMA: f"{self.catalog_name}.{self.schema_name}",
                catalog.SecurableType.CATALOG: self.catalog_name
            }
            full_name = full_name_map.get(securable_type)
            if not full_name:
                raise ValueError("Unsupported securable type")
            self.client.grants.update(
                securable_type=securable_type,
                full_name=full_name,
                changes=[changes]
            )