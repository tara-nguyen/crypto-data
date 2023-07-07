import pandas as pd
from staking.sources.subscan import SubscanExtractor


def get_data():
    """Retrieve pool names and pool IDs from Subscan and return a dataframe."""
    extractor = SubscanExtractor("/nomination_pool/pools")
    data = extractor.extract({})["list"]
    df = pd.json_normalize(data)

    df = df.reindex(columns=[
        "pool_id", "pool_account.address", "nominator_account.address",
        "nominator_account.parent.display",
        "nominator_account.parent.sub_symbol", "nominator_account.display"])
    df = df.set_axis(["id", "account", "nominator_acc", "pool", "subtitle",
                      "nominator_acc_title"], axis=1)
    df["total_bonded"] = df["total_bonded"].astype(int) / 1e10
    df["pool"] = df["pool"].str.cat(df["subtitle"], sep=" / ")
    df = df.fillna({"pool": df["nominator_acc_title"].mask(
        lambda x: x.isna(), df["nominator_acc"])})
    df = df.reindex(columns=["id", "pool", "account"])

    return df


if __name__ == "__main__":
    pools = get_data()
    with pd.option_context("display.max_columns", None):
        print(pools.set_index(["id", "pool"]).to_string())
    print(pools["total_bonded"].sum())
