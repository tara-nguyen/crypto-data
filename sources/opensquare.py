import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract, get_token_amount


class OpensquareExtractor:
    """Extract data from either SubSquare API or doTreasury API."""

    def __init__(self, source, network, route):
        self.method = "GET"

        networks = list(QuarterlyReport().networks.keys())
        network = network.lower()
        if network in networks:
            if source.lower() == "subsquare":
                self.url = f"https://{network}.subsquare.io/api{route}"
            elif source.lower() == "dotreasury":
                self.url = f"https://api.dotreasury.com/{network}{route}"
            else:
                raise Exception(
                    'source must be either "subsquare" or "dotreasury"')
        else:
            raise Exception(
                f'network must be either "{networks[0]}" or "{networks[1]}"')

    def extract(self):
        # The first extraction only returns the first page of data. If there is
        # more than one page, extract everything
        data = extract(self.method, self.url)
        if "pageSize" in data:
            data = extract(self.method, self.url,
                           params={"page_size": data["total"]})["items"]

        return data


class OpensquareTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data):
        self.data = data

    def to_frame(self, fields=None, token_cols=None, network=None):
        """
        Keyword arguments:
            fields: list of fields to keep in the dataframe
            token_cols: list of columns holding token amounts
            network: name of network (e.g., "polkadot", "kusama")
        """
        df = pd.json_normalize(self.data, sep="_")
        if fields is not None:
            df = df.reindex(columns=fields)

        if token_cols is not None:
            df[token_cols] = df[token_cols].applymap(
                lambda x: get_token_amount(x, network.lower()),
                na_action="ignore")

        df = df.sort_values(fields[0], ascending=False)

        return df
