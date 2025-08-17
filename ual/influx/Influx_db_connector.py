from influxdb_client import InfluxDBClient
import pandas as pd
from influxdb_client.client.flux_table import TableList


class InfluxDBConnector:
    def __init__(self, url: str, token: str, organization: str) -> None:
        """
        Initializes the InfluxDBConnector class.

        :param url: InfluxDB server URL
        :param token: InfluxDB authentication token
        :param organization: InfluxDB organization name
        """
        self.url = url
        self.token = token
        self.organization = organization
        self.timeout = 60000

        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.organization)
        self.query_api = self.client.query_api()

    def query(self, query: str) -> TableList:
        return self.query_api.query(query)

    def query_dataframe(self, query: str) -> pd.DataFrame:
        query_result = self.query_api.query_data_frame(query)
        query_result.drop(["result", "host", "topic", "table", "_start", "_stop", "_measurement"], inplace=True, axis=1)
        query_result.set_index("_time", inplace=True, drop=True)
        return query_result
