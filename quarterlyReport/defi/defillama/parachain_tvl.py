import pandas as pd
from sources.defillama import (DefillamaExtractor, DefillamaTransformer,
                               DefillamaParachains)


def get_data():
    df = pd.DataFrame([])
    for chain in DefillamaParachains().chains:
        data = DefillamaExtractor(f"/v2/historicalChainTvl/{chain}").extract()
        df_chain = DefillamaTransformer(data).to_frame()
        df_chain["chain"] = chain
        df = pd.concat([df, df_chain])

    df = df.pivot(index="date", columns="chain", values="tvl").fillna(0)
    df = df.sort_values(df.index[-1], axis=1, ascending=False)
    df = df.sort_index(ascending=False).reset_index()

    return df


if __name__ == "__main__":
    tvl = get_data()
    print(tvl.to_string())
