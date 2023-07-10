from reports.governance_etl import get_token_amount
from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["startIndexer_blockHeight", "startIndexer_blockTime",
              "endIndexer_blockHeight", "endIndexer_blockTime", "remaining",
              "proposals", "bounties", "tips", "burnt", "symbolPrice"]
    token_cols = ["remaining"]
    time_cols = ["startIndexer_blockTime", "endIndexer_blockTime"]

    data = Extractor("dotreasury", chain, "/periods").extract()
    df = Transformer(data).transform(fields, token_cols, chain, time_cols)

    df["proposals"] = df["proposals"].map(
        lambda ls: [d["proposalIndex"] for d in ls])
    df["bounties"] = df["bounties"].map(
        lambda ls: [d["bountyIndex"] for d in ls])
    df["tips"] = df["tips"].map(
        lambda ls: sum([get_token_amount(d["value"], chain) for d in ls]))
    df["burnt"] = df["burnt"].map(
        lambda x: get_token_amount(x[0]["value"], chain), na_action="ignore")

    return df


if __name__ == "__main__":
    spend = get_data("polkadot")
    print_long_df(spend)
