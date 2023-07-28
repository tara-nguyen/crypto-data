from parachain.sources.polkaholic import (PolkaholicExtractor,
                                          PolkaholicTransformer)


def get_data():
    data = PolkaholicExtractor(from_bigquery=False, route="/chains").extract({})
    df = PolkaholicTransformer(data).to_frame()

    df = df.query("relayChain in ['polkadot', 'kusama']").copy()
    df["relayChain"] = df["relayChain"].str.title()
    df["chainName"] = df["chainName"].str.title()
    df = df.replace(["Statemint", "Statemine"],
                    ['AssetHub-Polkadot', 'AssetHub-Kusama'])
    df = df.reindex(columns=["chainID", "chainName"])

    return df


if __name__ == "__main__":
    chains = get_data()
    print(chains.to_string())
