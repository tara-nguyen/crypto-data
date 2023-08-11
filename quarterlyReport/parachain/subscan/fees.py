import re
import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from sources.subscan import SubscanChains, SubscanExtractor
from sources.coingecko import CoingeckoCoins
from quarterlyReport.nft.coingecko import prices
from os import getcwd


def get_data(chains=None, start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    all_chains = SubscanChains().chains
    if chains is None:
        chains = all_chains
    elif isinstance(chains, str):
        chains = [chains]

    if isinstance(chains, list):
        chains = {chain: all_chains[chain] for chain in chains}

    path = re.match(".+crypto-data", getcwd()).group()
    path += "/nft/coingecko/prices_raw.csv"
    coins = CoingeckoCoins().coins_dict
    coin_ids = {coins[chain] for chain in chains if chain in coins}
    df_prices = prices.get_data(path, coin_ids, start, end)

    start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]
    df = pd.DataFrame([])
    for chain in chains:
        network = "Polkadot" if chain in SubscanChains().polkadot else "Kusama"
        payload = {"start": start, "end": end, "format": "day",
                   "category": "Fee"}
        data = SubscanExtractor("/daily", 2, network, chain).extract(payload)
        if data is not None:
            df_chain = pd.DataFrame(data)
            df_chain["chain"] = chain
            df_chain["total"] = (df_chain["total"].astype(float)
                                 / chains[chain][1])
            df = pd.concat([df, df_chain])

    df["date"] = pd.to_datetime(df["time_utc"]).dt.strftime("%Y-%m-%d")
    df = df.rename(columns={"total": "fee"})

    df = df.merge(df_prices).eval("feeUSD = fee * prices")
    df = df.reindex(columns=["chain", "feeUSD"]).groupby("chain").sum()
    df = df.sort_values("feeUSD", ascending=False).reset_index()

    return df


if __name__ == "__main__":
    fees = get_data()
    print(fees.to_string())
