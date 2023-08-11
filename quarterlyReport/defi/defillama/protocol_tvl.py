import pandas as pd
from sources.defillama import (DefillamaExtractor, DefillamaTransformer,
                               DefillamaParachains, filter_and_convert)
from quarterlyReport.defi.defillama import protocol_list


def get_data(category, network=None):
    df_protocols = protocol_list.get_data(category)

    if network is not None:
        network = network.lower()
        if network == "polkadot":
            chains = DefillamaParachains().polkadot
        elif network == "kusama":
            chains = DefillamaParachains().kusama
        else:
            raise Exception('network must be either "polkadot" or "kusama"')

        df_protocols = df_protocols.query("chain in @chains")

    df = pd.DataFrame([])
    for slug in df_protocols["slug"].unique():
        data = DefillamaExtractor(f"/protocol/{slug}").extract()["chainTvls"]

        for chain in df_protocols.query("slug == @slug")["chain"]:
            df_chain = DefillamaTransformer(data[chain]["tvl"]).to_frame()
            df_chain = filter_and_convert(df_chain)
            df_chain["chain"] = chain
            df_chain["slug"] = slug
            df = pd.concat([df, df_chain])

    df = df.merge(df_protocols).rename(columns={"totalLiquidityUSD": "tvl"})
    df = df.eval("name = name.str.cat(chain, ' - ')")
    df = df.pivot(index="date", columns="name", values="tvl")
    if category == "Dexes":
        if network == "polkadot":
            df["BeamSwap - Moonbeam"] = df.reindex(
                columns=["BeamSwap Classic - Moonbeam",
                         "BeamSwap Stable AMM - Moonbeam",
                         "BeamSwap V3 - Moonbeam"]).sum(axis=1)
            df["StellaSwap - Moonbeam"] = df.reindex(
                columns=["StellaSwap V2 - Moonbeam",
                         "StellaSwap V3 - Moonbeam"]).sum(axis=1)
            df = df.drop(columns=["BeamSwap Classic - Moonbeam",
                                  "BeamSwap Stable AMM - Moonbeam",
                                  "BeamSwap V3 - Moonbeam",
                                  "StellaSwap V2 - Moonbeam",
                                  "StellaSwap V3 - Moonbeam"])
        else:
            df["SushiSwap - Moonriver"] = df.reindex(
                columns=["SushiSwap - Moonriver",
                         "SushiSwap V3 - Moonriver"]).sum(axis=1)
            df = df.drop(columns="SushiSwap V3 - Moonriver")
    elif category == "Liquid Staking":
        df = df.drop(columns=["Acala LCDOT - Acala",
                              "Bifrost Liquid Crowdloan - Bifrost",
                              "Parallel Liquid Crowdloan - Heiko",
                              "Parallel Liquid Crowdloan - Parallel"])

    df = df.sort_values(df.index[-1], axis=1, ascending=False)
    chains_sorted = df.tail(1).columns.tolist()

    max_chain_count = 10
    if len(chains_sorted) > max_chain_count:
        df["Others"] = df.iloc[:, max_chain_count:].sum(axis=1)
    df = df.drop(columns=chains_sorted[max_chain_count:])
    df = df.interpolate().fillna(0)
    df = df.sort_index(ascending=False).reset_index()

    return df


if __name__ == "__main__":
    for network in ["polkadot", "kusama"]:
        dexes = get_data("Dexes", network)
        print()
        print(network.title())
        print(dexes.to_string())

    lending = get_data("Lending")
    print()
    print(lending.to_string())

    lqs = get_data("Liquid Staking")
    print()
    print(lqs.to_string())
