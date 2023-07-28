import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from parachain.sources.subscan import SubscanChains, SubscanExtractor


def get_data(ts=QuarterlyReport().end_time):
    ts = ts.strftime("%Y-%m-%d")

    df = pd.DataFrame([])
    for chain in SubscanChains().chains:
        network = "Polkadot" if chain in SubscanChains().polkadot else "Kusama"
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
    df = df.sort_values("holders", ascending=False)

    return df


if __name__ == "__main__":
    holders = get_data()
    print(holders.to_string())
