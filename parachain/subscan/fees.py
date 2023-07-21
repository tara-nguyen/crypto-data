import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from parachain.sources.subscan import SubscanNetworks, SubscanExtractor
from parachain.coingecko import prices


def get_data(chains=None, start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    polkadot_chains = SubscanNetworks().get_network("polkadot")
    kusama_chains = SubscanNetworks().get_network("kusama")
    all_chains = polkadot_chains | kusama_chains
    if chains is None:
        chains = all_chains
    elif isinstance(chains, str):
        chains = [chains]
    if isinstance(chains, list):
        chains = {chain: all_chains[chain] for chain in chains}

    coins = CoingeckoCoins().coins_dict
    coin_ids = {coins[chain] for chain in chains if chain in coins}
    df_prices = prices.get_data(coin_ids, start=start, end=end)
    start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]

    df = pd.DataFrame([])
    for chain in chains:
        network = "Polkadot" if chain in polkadot_chains else "Kusama"
        payload = {"start": start, "end": end, "format": "day",
                   "category": "Fee"}
        data = SubscanExtractor("/daily", 2, network, chain).extract(payload)
        if data is not None:
            df_chain = pd.DataFrame(data)
            df_chain["network"] = network
            df_chain["parachain"] = chain
            df_chain["total"] = (df_chain["total"].astype(float)
                                 / chains[chain][1])
            df = pd.concat([df, df_chain])

    df["date"] = pd.to_datetime(df["time_utc"]).dt.strftime("%Y-%m-%d")
    df = df.rename(columns={"total": "fee"})

    df = df.merge(df_prices, left_on=["parachain", "date"],
                  right_on=["chain", "date"])
    df = df.eval("feeUSD = fee / prices")
    df = df.reindex(columns=["network", "parachain", "feeUSD"])
    df = df.groupby(["network", "parachain"]).sum()
    df = df.sort_values("feeUSD", ascending=False).reset_index()

    return df


if __name__ == "__main__":
    print(pd.Timestamp.now())
    fees = get_data("Kusama")
    print(fees.to_string())
