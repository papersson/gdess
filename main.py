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

import pprint

# Assuming Tables.list returns a list of dictionaries
schema = Tables.get('uc', 'curated_elm_no', 'dim_absencestatus')
# pprint.pprint(schema.access.list().as_dict())
for p in schema.access.list():
    pprint.pprint(p)
