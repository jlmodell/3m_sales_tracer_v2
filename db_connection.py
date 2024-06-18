import yaml
from pydantic import BaseModel
from pymongo import MongoClient


def get_db_connection():
    with open("config.yaml", "r") as f:
        db_config = yaml.safe_load(f)
    return db_config


class Client:
    def __init__(self, uri: str):
        self.uri = uri
        self.client = MongoClient(self.uri)
        


DB_CONFIG = get_db_connection()
assert (
    DB_CONFIG["mongodb"]["uri"] is not None
), "MongoDB mongodb.uri is not set, check config.yaml"
BUSSE_SALES_DATA = DB_CONFIG["mongodb"]["databases"]["sales_data_warehouse"]["key"]
assert (
    BUSSE_SALES_DATA is not None
), "MongoDB databases.sales_data_warehouse.key is not set, check config.yaml"
DATA_WAREHOUSE = DB_CONFIG["mongodb"]["databases"]["sales_data_warehouse"]["collection"]
assert (
    DATA_WAREHOUSE is not None
), "MongoDB databases.sales_data_warehouse.collection is not set, check config.yaml"


client = Client(uri=DB_CONFIG["mongodb"]["uri"])
# client.connect()

dwh = client.client[BUSSE_SALES_DATA][DATA_WAREHOUSE]

if __name__ == "__main__":
    print(dwh)
