import pandas as pd
from governance_governanceReport.sources.onchain import initialize_substrate as init


def get_data(network):
    data = init(network).query_map("FellowshipCollective", "Members")
    df = pd.DataFrame([[d.value for d in dat] for dat in data],
                      columns=["member", "rank"])
    df["rank"] = df["rank"].map(lambda x: x["rank"])
    df = df.sort_values("rank", ascending=False)

    return df


if __name__ == "__main__":
    fellows = get_data("kusama")
    print(fellows)
