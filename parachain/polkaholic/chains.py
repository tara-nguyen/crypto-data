from parachain.sources.polkaholic import (PolkaholicExtractor,
                                          PolkaholicTransformer)


def get_data():
    data = PolkaholicExtractor(from_bigquery=False, route="/chains").extract({})
    df = PolkaholicTransformer(data).to_frame()

    df = df.query("relayChain in ['polkadot', 'kusama']").copy()
    df["relayChain"] = df["relayChain"].str.title()
    df["chainName"] = df["chainName"].str.title()
    df["paraID"] = df["chainID"].map(lambda x: x - 20000 if x > 20000 else x)
    df = df.replace(2, 0)
    df = df.reindex(columns=["relayChain", "paraID", "chainName"])

    return df


if __name__ == "__main__":
    chains = get_data()
    print(chains)
