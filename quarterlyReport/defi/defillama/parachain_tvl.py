import pandas as pd
from quarterlyReport.defi.sources.defillama import (
    DefillamaExtractor, DefillamaTransformer, DefillamaParachains,
    filter_and_convert)


def get_data():
    df = pd.DataFrame([])
    for chain in DefillamaParachains().chains:
        data = DefillamaExtractor(f"/v2/historicalChainTvl/{chain}").extract()
        df_chain = DefillamaTransformer(data).to_frame()
        df_chain = filter_and_convert(df_chain)
        df_chain["chain"] = chain
        df = pd.concat([df, df_chain])

    df = df.pivot(index="date", columns="chain", values="tvl").fillna(0)
    df = df.sort_values(df.index[-1], axis=1, ascending=False)
    df = df.sort_index(ascending=False).reset_index()

    return df


if __name__ == "__main__":
    tvl = get_data()
    print(tvl.to_string())
