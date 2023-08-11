import governanceReport.print_helpers as pr
import pandas as pd
from governanceReport.sources.opensquare import Extractor, Transformer


def get_data(network):
    fields = ["bountyIndex", "state", "proposer", "title", "content",
              "onchainData_description", "onchainData_value",
              "onchainData_meta_bond", "createdAt",
              "onchainData_meta_status_active_curator",
              "onchainData_meta_status_pendingPayout_curator",
              "onchainData_meta_curatorDeposit",
              "onchainData_meta_status_active_updateDue",
              "onchainData_timeline", "onchainData_state_indexer_blockTime",
              "onchainData_meta_status_pendingPayout_unlockAt", "commentsCount",
              "polkassemblyCommentsCount", "onchainData_motions"]
    token_cols = ["onchainData_value", "onchainData_meta_bond",
                  "onchainData_meta_curatorDeposit"]
    time_cols = ["onchainData_state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", network, "/treasury/bounties").extract()
    df = Transformer(data).transform(fields, token_cols, network, time_cols)

    return df


if __name__ == "__main__":
    bounties = get_data("kusama")
    df = pr.trim_long_strings(bounties, ["title", "content",
                                         "onchainData_description"])
    df = pr.trim_account_id_strings(
        df, ["proposer", "onchainData_meta_status_active_curator",
             "onchainData_meta_status_pendingPayout_curator"])
    pr.print_long_df(df)

    # special_col = "onchainData_timeline"
    # df = bounties.reindex(columns=["bountyIndex", special_col])
    # df = df.explode(special_col, ignore_index=True)
    # df2 = pd.json_normalize(df[special_col], sep="_")
    # df = pd.concat([df, df2], axis=1).drop(columns=special_col)
    # print(df.to_string())
