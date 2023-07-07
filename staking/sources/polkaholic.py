import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from staking import StakingReport


class PolkaholicExtractor:
    def __init__(self, credentials="service-account.json"):
        credentials = service_account.Credentials.from_service_account_file(
            credentials)
        self.client = bigquery.Client(credentials=credentials)

    def extract(self, query_template, start=StakingReport().start_time,
                end=StakingReport().end_time, **kwargs):
        """Extract data from Polkaholic."""
        query = query_template.substitute(
            start=pd.Timestamp(start).strftime("%Y-%m-%d"),
            end=pd.Timestamp(end).strftime("%Y-%m-%d"), **kwargs)
        query_job = self.client.query(query)
        df = query_job.to_dataframe()

        return df
