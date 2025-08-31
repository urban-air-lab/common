from influxdb_client import InfluxDBClient, QueryApi
import pandas as pd
from influxdb_client.client.flux_table import TableList
from ual.logging import get_logger


class InfluxDBConnector:
    def __init__(self, url: str, token: str, organization: str) -> None:
        """
        Initializes the InfluxDBConnector class.

        :param url: InfluxDB server URL
        :param token: InfluxDB authentication token
        :param organization: InfluxDB organization name
        """
        self.url: str = url
        self.token: str = token
        self.organization: str = organization
        self.timeout: int = 60000

        self.client: InfluxDBClient = InfluxDBClient(url=self.url, token=self.token, org=self.organization)
        self.query_api: QueryApi = self.client.query_api()

        self.logger = get_logger()

    def query(self, query: str) -> TableList:
        try:
            return self.query_api.query(query)
        except ConnectionError as e:
            self.logger.error("Exception occurred for Influx request", exc_info=True)

    def query_dataframe(self, query: str) -> pd.DataFrame:
        try:
            query_result: pd.DataFrame = self.query_api.query_data_frame(query)
            if not query_result.empty:
                query_result.drop(["result", "host", "topic", "table", "_start", "_stop", "_measurement"], inplace=True, axis=1)
                query_result.set_index("_time", inplace=True, drop=True)
            return query_result
        except ConnectionError as e:
            self.logger.error("Exception occurred for Influx request", exc_info=True)



