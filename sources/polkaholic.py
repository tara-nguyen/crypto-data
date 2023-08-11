import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from reports.quarterly_etl import QuarterlyReport, extract


class PolkaholicExtractor:
    """Extract data from Polkaholic, either via Big Query or directly from
    Polkaholic API.
    """

    def __init__(self, from_bigquery=True, credentials="service-account.json",
                 route=""):
        self.from_bigquery = from_bigquery
        if from_bigquery:
            credentials = service_account.Credentials.from_service_account_file(
                credentials)
            self.client = bigquery.Client(credentials=credentials)
        else:
            self.method = "GET"
            self.url = f"https://api.polkaholic.io{route}"

    def extract_bigquery(self, query_template,
                         start=QuarterlyReport().start_time,
                         end=QuarterlyReport().end_time, **kwargs):
        """
        Keyword arguments:
            start: start point of the time range of interest
            end: end point of the time range of interest
        """
        start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]
        query = query_template.substitute(start=start, end=end, **kwargs)
        data = self.client.query(query)

        return data

    def extract_api(self, params):
        data = extract(self.method, self.url, params=params)

        return data

    def extract(self, *args, **kwargs):
        if self.from_bigquery:
            data = self.extract_bigquery(*args, **kwargs)
        else:
            data = self.extract_api(*args)

        return data


class PolkaholicTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data):
        self.data = data

    def to_frame(self):
        if isinstance(self.data, bigquery.QueryJob):
            df = self.data.to_dataframe()
        else:
            df = pd.json_normalize(self.data, sep="_")

        return df
