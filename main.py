import os
from dotenv import load_dotenv

load_dotenv()

os.environ['DATABRICKS_HOST'] = os.getenv('DATABRICKS_HOST_DEV')
os.environ['DATABRICKS_CLIENT_ID'] = os.getenv('DATABRICKS_CLIENT_ID')
os.environ['DATABRICKS_CLIENT_SECRET'] = os.getenv('DATABRICKS_CLIENT_SECRET')

from self_service.unitycatalog.asset import Schemas, Catalogs

# from databricks.sdk import WorkspaceClient
# c = WorkspaceClient()
# print(c.catalogs.list())

# Delete catalogs
# Catalogs.delete('elm_dev')

# Create catalogs
# Catalogs.create('elm', 'dev')
# Catalogs.create('elm', 'uat')
# Catalogs.create('elm', 'prod')

# Create schemas
Schemas.create('elm_dev', 'curated_sundhedspartner')


# from self_service.unitycatalog.devops.deploy import deploy_uat
# deploy_uat('elm')

# Catalogs.get('elm_dev').access.grant('readwrite', 'ELM_DataEngineers')