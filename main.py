import os
from dotenv import load_dotenv
from self_service.unitycatalog.asset import Schemas
import os

load_dotenv()

# os.environ['DATABRICKS_HOST'] = os.getenv('DATABRICKS_HOST_UAT')
os.environ['DATABRICKS_CLIENT_ID'] = os.getenv('DATABRICKS_CLIENT_ID')
os.environ['DATABRICKS_CLIENT_SECRET'] = os.getenv('DATABRICKS_CLIENT_SECRET')

# from databricks.sdk import WorkspaceClient
# c = WorkspaceClient()
# print(c.catalogs.list())



from self_service.unitycatalog.devops.deploy import deploy_uat
deploy_uat('elm')

