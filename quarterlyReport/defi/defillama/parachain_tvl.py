from quarterlyReport.defi.sources.defillama import *


def get_data():
    df = pd.DataFrame([])
    for chain in DefillamaParachains().chains:
        data = DefillamaExtractor(f"/v2/historicalChainTvl/{chain}").extract()
        df_chain = DefillamaTransformer(data).to_frame()
        df_chain = filter_and_convert(df_chain)
        df_chain["chain"] = chain
        df = pd.concat([df, df_chain])

    df_chains_sorted = df.groupby("chain").tail(1)
    df_chains_sorted = df_chains_sorted.sort_values("tvl", ascending=False)
    chains_sorted = df_chains_sorted["chain"].unique()

    df = df.pivot(index="date", columns="chain", values="tvl")
    df = df.fillna(0).reindex(columns=chains_sorted)
    df = df.sort_index(ascending=False).reset_index()

    return df


if __name__ == "__main__":
    tvl = get_data()
    print(tvl)
