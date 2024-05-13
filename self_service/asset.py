from databricks.sdk import WorkspaceClient
from .access import CatalogAccess, SchemaAccess, TableAccess
from .metadata import Metadata

class Catalog:
    def __init__(self, business_unit: str, environment: str):
        self.client = WorkspaceClient()
        self.full_name = f"{business_unit}_{environment}"
        self.access = CatalogAccess(self.full_name)
        self.metadata = Metadata(self.full_name)

    def create(self):
        """
        Create a catalog.
        """
        return self.client.catalogs.create(name=self.full_name)

    def delete(self):
        """
        Delete a catalog.
        """
        return self.client.catalogs.delete(name=self.full_name, force=True)

class Schema:
    def __init__(self, catalog_name: str, schema_name: str, data_layer: str):
        self.client = WorkspaceClient()
        self.catalog_name = catalog_name
        self.schema_name = f'{data_layer}_{schema_name}'
        self.access = SchemaAccess(self.catalog_name, self.schema_name)
        self.metadata = Metadata(f'{self.catalog_name}.{self.schema_name}')

    def create(self):
        """
        Create a schema.
        """
        return self.client.schemas.create(name=self.schema_name, catalog_name=self.catalog_name)

    def delete(self):
        """
        Delete a schema.
        """
        return self.client.schemas.delete(full_name=f'{self.catalog_name}.{self.schema_name}')

class Table:
    def __init__(self, catalog_name: str, schema_name: str, table_name: str):
        self.access = TableAccess(catalog_name, schema_name, table_name)
        self.metadata = Metadata(f'{catalog_name}.{schema_name}.{table_name}')