import json

import boto3
import msal
import pandas as pd
import requests
from botocore.config import Config
from google.cloud import bigquery


class S3Ops:
""" 
This class consists of methods working with files in AWS S3 
"""
    def __init__(self, bucket: str, key: str, columns: str = None) -> None:
        """ 
        Instanciate an s3 session and a resource instance with default configs 
        """
        self.bucket = bucket
        self.key = key
        self.columns = columns
        self.s3_config = Config(s3={"use_accelerate_endpoint": True})
        self.session = boto3.Session()
        self.s3 = self.session.resource("s3", config=self.s3_config)

    def get_data(self) -> pd.DataFrame:
        """ Download json file as streaming object before reading into a Pandas DataFrame """
        obj = self.s3.Object(self.bucket, self.key)
        json_data = json.loads(obj.get()["Body"].read().decode())
        if self.columns is None:
            data_dict = json.dumps(json_data)
            df = pd.DataFrame(json_data)
        else:
            data_dict = json.dumps(
                [{col: entry[col] for col in self.columns} for entry in json_data]
            )
            df = pd.read_json(data_dict)
        return df


class BigQueryOps:
    """ 
    This class consists of methods working with tables in Google Cloud BigQuery 
    """
    def __init__(self, table: str) -> None:
        """ Instanciate a BigQuery instance with the declared table """
        self.client = bigquery.Client()
        try:
            self.table = self.client.get_table(table)
        except Exception as e:
            raise ValueError(f"Error fetching table: {e}")

    def get_columns(self) -> list:
        """ 
        Retrive a list columns of the table 
        """
        if self.table:
            self.columns = [field.name for field in self.table.schema]
            return self.columns
        else:
            raise ValueError("Error fetching columns!")

    def columns_string(self) -> str:
        """ 
        Retrieve a list of column headers of the table as string.
        """
        if self.columns:
            self.columns_str = ", ".join(self.columns)
            return self.columns_str
        else:
            return ""

    def get_schema(self) -> list:
        """
        Retrieve a list of columns and data types
        """
        if self.table:
            self.schema = self.table.schema
            return self.schema
        else:
            return []

    def get_schema_as_dict(self) -> dict:
        """
        Retrieve a dictionary of columns and data types
        """
        self.schema_dict = {}
        if self.table:
            for field in self.table.schema:
                self.schema_dict[field.name] = field.field_type
        return self.schema_dict

    def get_table_id(self) -> str:
        """
        Retrieve a string of fully qualified table id in BigQuery
        Ex: activeTix.raw.raw_sale_orders
        """
        if self.table:
            self.table_id = str(
                f"{self.table.project}.{self.table.dataset_id}.{self.table.table_id}"
            )
            return self.table_id
        else:
            return ""

    def default_config(self) -> bigquery.LoadJobConfig:
        """
        Set default configs for loading table job
        """
        return bigquery.LoadJobConfig(
            schema=self.get_schema(), write_disposition="WRITE_TRUNCATE"
        )

    def load_table(self, dataframe: pd.DataFrame) -> None:
        """
        Save a Pandas DataFrame as a BigQuery table.
        By default, existing table will be overwritten.
        """
        if not hasattr(self, "table_id"):
            self.get_table_id()
        try:
            job = self.client.load_table_from_dataframe(
                dataframe=dataframe,
                destination=self.table_id,
                job_config=self.default_config(),
            )
            job.result()
            print(f"Table successfully loaded to {self.table_id}")
        except Exception as e:
            print(f"Error loading table: {str(e)}")


class PowerBIOps:
    def __init__(
        self,
        tenant_id: str,
        app_id: str,
        dataset_id: str,
        username: str,
        password: str,
    ) -> None:
        self.tenant_id = tenant_id
        self.app_id = app_id
        self.dataset_id = dataset_id
        self.username = username
        self.password = password

    def get_token(self) -> str:
        authority_url = "https://login.microsoftonline.com/" + self.tenant_id
        scopes = ["https://analysis.windows.net/powerbi/api/.default"]
        client = msal.PublicClientApplication(self.app_id, authority=authority_url)
        token_response = client.acquire_token_by_username_password(
            username=self.username, password=self.password, scopes=scopes
        )
        if "access_token" not in token_response:
            raise Exception(token_response["error_description"])
        self.access_id = token_response.get("access_token")
        return self.access_id

    def refresh_dataset(self) -> None:
        if not hasattr(self, "access_id"):
            self.get_token()
            endpoint = f"https://api.powerbi.com/v1.0/myorg/datasets/{self.dataset_id}/refreshes"
            headers = {"Authorization": "Bearer " + self.access_id}
            response = requests.post(endpoint, headers=headers)
        if response.status_code == 202:
            print("Dataset refreshed")
        else:
            raise Exception
