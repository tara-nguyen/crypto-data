import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from nft.sources.moonbeans import MoonbeansExtractor, MoonbeansTransformer
from nft.moonbeans import collections
from nft.coingecko import prices
from string import Template


def get_data(start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    """Retrieve trade data from Moonbeans and return a dataframe."""
    template = Template(
        '{"query": "query ALL_TRADES {allFills(orderBy: TIMESTAMP_DESC, '
        'filter: {timestamp: {greaterThanOrEqualTo: \\"$start\\", '
        'lessThan: \\"$end\\"}}) {nodes {collectionId timestamp value}}}", '
        '"operationName": "ALL_TRADES"}')

    data = MoonbeansExtractor(True).extract(template)
    df = MoonbeansTransformer(data["allFills"]["nodes"]).to_frame()

    df["date"] = pd.to_datetime(df["timestamp"].astype(int),
                                unit="s").dt.strftime("%Y-%m-%d")
    df["value"] = df["value"].astype(float) / 1e18
    df = df.merge(collections.get_data(), "left", left_on="collectionId",
                  right_on="contractAddress")

    chains = df["chain"].unique().tolist()
    df_prices = prices.get_data(chains, start=start, end=end)
    df_prices["chain"] = df_prices["chain"].str.lower()
    df = df.merge(df_prices).eval("volume = value * prices")
    df = df.reindex(columns=["date", "chain", "volume"])

    dates = pd.date_range(start, end, freq="1D").strftime("%Y-%m-%d")[:-1]
    index = pd.MultiIndex.from_product([dates, chains], names=["date", "chain"])
    df = df.groupby(index.names).sum().reindex(index=index, fill_value=0)
    df.reset_index(inplace=True)

    return df


if __name__ == "__main__":
    trades = get_data()
    print(trades.to_string())
