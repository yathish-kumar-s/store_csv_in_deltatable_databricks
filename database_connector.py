import os
from databricks import sql
def db_connector():
    connection = sql.connect(
        server_hostname="adb-3329291283320168.8.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/cb88f513b9e11404",
        access_token=os.getenv('DATABRICKS_ACCESS_TOKEN'))

    return connection