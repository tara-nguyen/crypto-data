import governance.print_helpers as pr
import pandas as pd
from governance.sources.opensquare import Extractor, Transformer


def get_data(chain):
    fields = ["proposalIndex", "referendumIndex", "track", "state", "proposer",
              "onchainData_beneficiary", "title", "content",
              "onchainData_value", "onchainData_meta_bond", "createdAt",
              "onchainData_state_indexer_blockTime", "commentsCount",
              "polkassemblyCommentsCount", "onchainData_motions",
              "onchainData_referendums"]
    token_cols = ["onchainData_value", "onchainData_meta_bond"]
    time_cols = ["onchainData_state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", chain, "/treasury/proposals").extract()
    df = Transformer(data).transform(fields, token_cols, chain, time_cols)

    return df


if __name__ == "__main__":
    proposals = get_data("kusama")
    df = pr.trim_long_strings(proposals, ["title", "content"])
    df = pr.trim_account_id_strings(df, ["proposer", "onchainData_beneficiary"])
    pr.print_long_df(df)

    # special_col = "onchainData_motions"
    # df = proposals.reindex(columns=["proposalIndex", "createdAt", special_col])
    # df = df.explode(special_col, ignore_index=True)
    # df2 = pd.json_normalize(df[special_col], sep="_")
    # df = pd.concat([df, df2], axis=1).drop(columns=special_col)
    # print(df.to_string())
