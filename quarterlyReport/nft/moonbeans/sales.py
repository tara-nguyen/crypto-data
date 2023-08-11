import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from quarterlyReport.nft.sources.moonbeans import MoonbeansExtractor
from quarterlyReport.nft.sources.moonbeans import MoonbeansTransformer
from quarterlyReport.nft.moonbeans import collections
from quarterlyReport.nft.coingecko import prices
from string import Template


def get_data(price_file_path="../coingecko/prices_raw.csv",
             start=QuarterlyReport().start_time,
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
    df_prices = prices.get_data(price_file_path, chains, start, end)
    df_prices["chain"] = df_prices["chain"].str.lower()
    df = df.merge(df_prices).eval("totalUSD = value * prices")
    df = df.reindex(columns=["date", "chain", "totalUSD"])

    dates = pd.date_range(start, end, freq="1D").strftime("%Y-%m-%d")[:-1]
    index = pd.MultiIndex.from_product([dates, chains], names=["date", "chain"])
    df = df.groupby(index.names).sum().reindex(index=index, fill_value=0)
    df.reset_index(inplace=True)

    return df


if __name__ == "__main__":
    trades = get_data()
    print(trades)
