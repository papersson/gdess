import pytest
import time
from self_service.unitycatalog import UnityCatalog
import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient, AccountClient

# Load environment variables
load_dotenv()

# Use environment variables
DATABRICKS_HOST_WORKSPACE = os.getenv("DATABRICKS_HOST_WORKSPACE")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATABRICKS_HOST_ACCOUNT = os.getenv("DATABRICKS_HOST_ACCOUNT")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

# Initialize the WorkspaceClient and AccountClient with environment variables
workspace_client = WorkspaceClient(host=DATABRICKS_HOST_WORKSPACE, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
account_client = AccountClient(host=DATABRICKS_HOST_ACCOUNT, account_id=ACCOUNT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)


@pytest.fixture(scope="module")
def unity_catalog():
    workspace_client = WorkspaceClient(host=DATABRICKS_HOST_WORKSPACE, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    account_client = AccountClient(host=DATABRICKS_HOST_ACCOUNT, account_id=ACCOUNT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    return UnityCatalog(workspace_client, account_client)

def test_create_and_delete_catalog(unity_catalog):
    catalog_name = f'integration_test_catalog_{int(time.time())}'
    # Create catalog
    catalog_info = unity_catalog.create_catalog(catalog_name)
    assert catalog_info is not None, "Catalog creation failed"
    print(f"Catalog {catalog_name} created successfully.")

    # Cleanup: Delete catalog
    try:
        unity_catalog.delete_catalog(catalog_name)
        print(f"Catalog {catalog_name} deleted successfully.")
    except Exception as e:
        print(f"Failed to delete catalog {catalog_name}: {e}")

