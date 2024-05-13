import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient, AccountClient
from self_service.unitycatalog import UnityCatalog
from self_service.client.unitycatalog import UnityCatalogClient
from self_service.access import SchemaAccess, CatalogAccess, TableAccess

access = TableAccess(catalog_name='uc', schema_name='curated_elm_no', table_name='dim_absencestatus')
access.revoke('read', 'toni.krmek@falck.com')

# access = SchemaAccess(catalog_name='uc', schema_name='curated_elm_no')
# access.revoke('read', 'toni.krmek@falck.com')

