from reports.quarterly_etl import QuarterlyReport


def get_data():
    data = PolkaholicExtractor(from_bigquery=False, route="/chains").extract({})
    df = PolkaholicTransformer(data).to_frame()

    relay_chains = QuarterlyReport().networks
    df = df.query("relayChain in @relay_chains").copy()
    df["relayChain"] = df["relayChain"].str.title()
    df["chainName"] = df["chainName"].str.title()
    df = df.replace(["Statemint", "Statemine"],
                    ["Asset Hub-Polkadot", "Asset Hub-Kusama"])
    df = df.reindex(columns=["chainID", "chainName"])

    return df


if __name__ == "__main__":
    chains = get_data()
    print(chains.to_string())
