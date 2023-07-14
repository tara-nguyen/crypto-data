import pandas as pd
from reports.quarterly_etl import QuarterlyReport, convert_timestamp
from parachain.sources.subscan import SubscanNetworks, SubscanExtractor
from parachain.coingecko import prices


def get_data(start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    start, end = [convert_timestamp(t, "%Y-%m-%d") for t in [start, end]]
    polkadot_chains = SubscanNetworks().get_network("polkadot")
    kusama_chains = SubscanNetworks().get_network("kusama")
    chains = polkadot_chains | kusama_chains

    df = pd.DataFrame([])
    for chain in chains:
        network = "Polkadot" if chain in polkadot_chains else "Kusama"
        payload = {"start": start, "end": end, "format": "day",
                   "category": "Fee"}
        data = SubscanExtractor("/daily", 2, network, chain).extract(payload)
        df_chain = pd.DataFrame(data)
        df_chain["total"] = df_chain["total"].astype(float) / chains[chain][1]
        df_chain["network"] = network
        df_chain["parachain"] = chain
        df = pd.concat([df, df_chain])

    df["date"] = df["time_utc"].map(lambda t: convert_timestamp(t))
    df = df.rename(columns={"total": "fee"})
    df = df.merge(prices.get_data(start, end), left_on=["parachain", "date"],
                  right_on=["chain", "date"])
    df = df.eval("feeUSD = fee / prices")
    df = df.reindex(columns=["network", "parachain", "date", "feeUSD"])

    return df


if __name__ == "__main__":
    print(pd.Timestamp)
    fees = get_data()
    print(fees)
