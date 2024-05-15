from databricks.sdk import WorkspaceClient
from .access import CatalogAccess, SchemaAccess, TableAccess
from .metadata import Metadata

class Catalog:
    def __init__(self, full_name: str):
        self._full_name = full_name
        self.access = CatalogAccess(self._full_name)
        self.metadata = Metadata(self._full_name)

    def __repr__(self):
        return f'Catalog(catalog_name={self._full_name})'

class Catalogs:
    _client = WorkspaceClient().catalogs

    @classmethod
    def create(cls, name: str):
        return cls._client.create(name=name)

    @classmethod
    def delete(cls, name: str):
        return cls._client.delete(name=name, force=True)

    @classmethod
    def list(cls):
        catalog_infos = cls._client.list()
        return [Catalog(full_name=info['name']) for info in catalog_infos]

    @classmethod
    def get(cls, name: str) -> Catalog:
        catalog_info = cls._client.get(name=name)
        if catalog_info:
            return Catalog(full_name=name)
        else:
            raise ValueError("Catalog not found")

class Schema:
    def __init__(self, catalog_name: str, schema_name: str):
        self._catalog_name = catalog_name
        self.schema_name = schema_name
        self.access = SchemaAccess(self._catalog_name, self.schema_name)
        self.metadata = Metadata(f'{self._catalog_name}.{self.schema_name}')

    def __repr__(self):
        return f'Schema(catalog_name={self._catalog_name}, schema_name={self.schema_name})'

class Schemas:
    _client = WorkspaceClient().schemas

    @classmethod
    def create(cls, catalog_name: str, schema_name: str):
        return cls._client.create(name=schema_name, catalog_name=catalog_name)

    @classmethod
    def delete(cls, catalog_name: str, schema_name: str):
        return cls._client.delete(full_name=f'{catalog_name}.{schema_name}')

    @classmethod
    def list(cls, catalog_name: str):
        schema_infos = cls._client.list(catalog_name=catalog_name)
        return [Schema(catalog_name=catalog_name, schema_name=info['name']) for info in schema_infos]

    @classmethod
    def get(cls, catalog_name: str, schema_name: str) -> Schema:
        schema_info = cls._client.get(full_name=f'{catalog_name}.{schema_name}')
        if schema_info:
            return Schema(catalog_name=catalog_name, schema_name=schema_name)
        else:
            raise ValueError("Schema not found")
        
class Table:
    def __init__(self, catalog_name: str, schema_name: str, table_name: str):
        self._catalog_name = catalog_name
        self._schema_name = schema_name
        self._table_name = table_name
        self.access = TableAccess(catalog_name, schema_name, table_name)
        self.metadata = Metadata(f'{catalog_name}.{schema_name}.{table_name}')

    def __repr__(self):
        return f'Table(catalog_name={self._catalog_name}, schema_name={self._schema_name}, table_name={self._table_name})'

class Tables:
    _client = WorkspaceClient().tables

    @classmethod
    def create(cls, catalog_name: str, schema_name: str, table_name: str):
        raise NotImplementedError("Create method for Table is not implemented yet.")

    @classmethod
    def delete(cls, catalog_name: str, schema_name: str, table_name: str):
        raise NotImplementedError("Delete method for Table is not implemented yet.")

    @classmethod
    def list(cls, catalog_name: str, schema_name: str):
        table_infos = cls._client.list(catalog_name=catalog_name, schema_name=schema_name)
        return [Table(catalog_name=catalog_name, schema_name=schema_name, table_name=info.name) for info in table_infos]

    @classmethod
    def get(cls, catalog_name: str, schema_name: str, table_name: str) -> Table:
        table_info = cls._client.get(full_name=f'{catalog_name}.{schema_name}.{table_name}')
        if table_info:
            return Table(catalog_name=catalog_name, schema_name=schema_name, table_name=table_name)
        else:
            raise ValueError("Table not found")