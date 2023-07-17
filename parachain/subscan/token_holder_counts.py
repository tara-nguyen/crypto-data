import pandas as pd
from reports.quarterly_etl import QuarterlyReport, convert_timestamp
from parachain.sources.subscan import SubscanNetworks, SubscanExtractor


def get_data(start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    start, end = [convert_timestamp(t, "%Y-%m-%d") for t in [start, end]]
    polkadot_chains = SubscanNetworks().get_network("polkadot")
    kusama_chains = SubscanNetworks().get_network("kusama")

    df = pd.DataFrame([])
    for chain in polkadot_chains | kusama_chains:
        network = "Polkadot" if chain in polkadot_chains else "Kusama"
        payload = {"start": start, "end": end, "format": "day",
                   "category": "AccountHolderTotal"}
        data = SubscanExtractor("/daily", 2, network, chain).extract(payload)
        df_chain = pd.DataFrame(data)
        df_chain["network"] = network
        df_chain["parachain"] = chain
        df = pd.concat([df, df_chain])

    df["date"] = df["time_utc"].map(lambda t: convert_timestamp(t))
    df = df.rename(columns={"total": "accounts"})
    df = df.reindex(columns=["network", "parachain", "date", "accounts"])

    return df


if __name__ == "__main__":
    accounts = get_data()
    print(accounts)
