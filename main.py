import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient, AccountClient
from self_service.unitycatalog import UnityCatalog
from self_service.client.unitycatalog import UnityCatalogClient
from self_service.access import SchemaAccess, CatalogAccess, TableAccess
from self_service.asset import Catalog, Schema
from self_service.metadata import Metadata
import os

load_dotenv()

os.environ['DATABRICKS_HOST'] = os.getenv('DATABRICKS_HOST')
os.environ['DATABRICKS_CLIENT_ID'] = os.getenv('DATABRICKS_CLIENT_ID')
os.environ['DATABRICKS_CLIENT_SECRET'] = os.getenv('DATABRICKS_CLIENT_SECRET')

# access = TableAccess(catalog_name='uc', schema_name='curated_elm_no', table_name='dim_absencestatus')
# access.revoke('read', 'toni.krmek@falck.com')

catalog = Catalog('test123', 'dev')
# catalog.create()
# catalog.access.grant('read', 'toni.krmek@falck.com')
catalog.metadata.add_comment('This is a brand new comment, WOW.')
from databricks.sdk import WorkspaceClient
client = WorkspaceClient()
ci = client.catalogs.get(f'test123_dev')
print(ci.as_dict())

