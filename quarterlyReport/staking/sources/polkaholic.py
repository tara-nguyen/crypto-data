from google.cloud import bigquery
from google.oauth2 import service_account
from reports.quarterly_etl import QuarterlyReport


class PolkaholicExtractor:
    def __init__(self, credentials="service-account.json"):
        credentials = service_account.Credentials.from_service_account_file(
            credentials)
        self.client = bigquery.Client(credentials=credentials)

    def extract(self, query_template, start=QuarterlyReport().start_time,
                end=QuarterlyReport().end_time, **kwargs):
        """Extract data from Polkaholic's Big Query dataset."""
        start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]
        query = query_template.substitute(start=start, end=end, **kwargs)
        data = self.client.query(query)

        return data


class PolkaholicTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self):
        df = self.data.to_dataframe()

        return df
