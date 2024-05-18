from databricks.sdk import WorkspaceClient
from self_service.unitycatalog.asset import Schemas
import os
from dotenv import load_dotenv
load_dotenv()

DATABRICKS_HOST_DEV = os.getenv("DATABRICKS_HOST_DEV")
DATABRICKS_HOST_UAT = os.getenv("DATABRICKS_HOST_UAT")
DATABRICKS_HOST_PROD = os.getenv("DATABRICKS_HOST_PROD")
DATABRICKS_CLIENT_ID = os.getenv('DATABRICKS_CLIENT_ID')
DATABRICKS_CLIENT_SECRET = os.getenv('DATABRICKS_CLIENT_SECRET')

def get_client(environment):
    if environment == "dev":
        return WorkspaceClient(host=DATABRICKS_HOST_DEV).schemas
    elif environment == "uat":
        return WorkspaceClient(host=DATABRICKS_HOST_UAT).schemas
    elif environment == "prod":
        return WorkspaceClient(host=DATABRICKS_HOST_PROD).schemas
    else:
        raise ValueError(f"Unknown environment: {environment}")

def get_catalog_name(business_unit, environment):
    if environment == 'prod':
        return business_unit
    else:
        return f'{business_unit}_{environment}'

def sync_schemas(source_environment, target_environment, business_unit):
    source_client = get_client(source_environment)
    target_client = get_client(target_environment)
    source_catalog_name = get_catalog_name(business_unit, source_environment)
    target_catalog_name = get_catalog_name(business_unit, target_environment)

    source_schemas = {schema.name for schema in source_client.list(catalog_name=source_catalog_name)}
    target_schemas = {schema.name for schema in target_client.list(catalog_name=target_catalog_name)}
    missing_schemas = source_schemas - target_schemas

    for schema_name in missing_schemas:
        target_client.create(catalog_name=target_catalog_name, name=schema_name)
        print(f"Schema {schema_name} created in {business_unit} catalog in target environment.")

def deploy_uat(business_unit):
    print("Deploying schemas from DEV to UAT...")
    sync_schemas("dev", "uat", business_unit)
    print("Deployment to UAT completed.")

def deploy_prod(business_unit):
    print("Deploying schemas from UAT to PROD...")
    sync_schemas("uat", "prod", business_unit)
    print("Deployment to PROD completed.")