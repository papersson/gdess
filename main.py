import os
from dotenv import load_dotenv
from self_service.asset import Catalogs, Schemas, Tables
import os

load_dotenv()

os.environ['DATABRICKS_HOST'] = os.getenv('DATABRICKS_HOST')
os.environ['DATABRICKS_CLIENT_ID'] = os.getenv('DATABRICKS_CLIENT_ID')
os.environ['DATABRICKS_CLIENT_SECRET'] = os.getenv('DATABRICKS_CLIENT_SECRET')

# table = Tables.get('uc', 'curated_elm_no', 'dim_absencestatus')
# table.access.revoke('read', 'niels.bleijerveld@falck.com')

# import pprint

# Catalogs.create('hr', 'uat')

from databricks.sdk import WorkspaceClient

# # Initialize the workspace client
# w = WorkspaceClient()

# # Get the current workspace ID
# current_workspace_id = w.get_workspace_id()
# print(f'Current Workspace ID: {current_workspace_id}')

Catalogs.delete('hr_dev')
Catalogs.create('hr', 'dev')
Catalogs.create('hr', 'uat')
# Catalogs.delete('hr_uat')
# Catalogs.delete('hr_dev')
