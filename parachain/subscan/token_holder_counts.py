import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from parachain.sources.subscan import SubscanNetworks, SubscanExtractor


def get_data(ts=QuarterlyReport().end_time):
    ts = ts.strftime("%Y-%m-%d")
    polkadot_chains = SubscanNetworks().get_network("polkadot")
    kusama_chains = SubscanNetworks().get_network("kusama")

    df = pd.DataFrame([])
    for chain in polkadot_chains | kusama_chains:
        network = "Polkadot" if chain in polkadot_chains else "Kusama"
        payload = {"start": ts, "end": ts, "format": "day",
                   "category": "AccountHolderTotal"}
        data = SubscanExtractor("/daily", 2, network, chain).extract(payload)
        df_chain = pd.DataFrame(data)
        df_chain["network"] = network
        df_chain["parachain"] = chain
        df = pd.concat([df, df_chain])

    df["date"] = df["time_utc"].str.slice(stop=10)
    df = df.rename(columns={"total": "holders"})
    df = df.reindex(columns=["network", "parachain", "holders"])
    df["holders"] = df["holders"].astype(int)
    df = df.query("holders > 2e4").sort_values("holders", ascending=False)

    return df


if __name__ == "__main__":
    holders = get_data()
    print(holders.to_string())
