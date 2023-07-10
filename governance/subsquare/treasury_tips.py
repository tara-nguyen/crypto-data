import governance.print_helpers as pr
from reports.governance_etl import get_token_amount
from governance.sources.opensquare import Extractor, Transformer


def get_data(chain):
    fields = ["hash", "state_state", "onchainData_timeline", "finder",
              "onchainData_meta_who", "title", "content", "onchainData_reason",
              "onchainData_meta_deposit", "createdAt", "height",
              "state_tipsCount", "onchainData_meta_tips",
              "onchainData_medianValue", "onchainData_state_indexer_blockTime",
              "commentsCount", "polkassemblyCommentsCount"]
    token_cols = ["onchainData_meta_deposit", "onchainData_medianValue"]
    time_cols = ["onchainData_state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", chain, "/treasury/tips").extract()
    df = Transformer(data).transform(fields, token_cols, chain, time_cols,
                                     sort_by="createdAt", ascending=False)

    df["onchainData_timeline"] = df["onchainData_timeline"].map(
        lambda ls: ls[0]["method"])

    new_col = "tipper"
    target_col = "onchainData_meta_tips"
    df[new_col] = df[target_col].map(lambda ls: [l[0] for l in ls])
    df[target_col] = df[target_col].map(
        lambda ls: [get_token_amount(l[1], chain) for l in ls])

    cols = fields.copy()
    cols.insert(fields.index(target_col), new_col)
    df = df.reindex(columns=cols)
    df = df.rename(columns={target_col: "tipAmounts",
                            "onchainData_timeline": "callMethod"})

    return df


if __name__ == "__main__":
    tips = get_data("kusama")
    df = pr.trim_long_strings(tips, ["hash", "title", "content",
                                     "onchainData_reason"])
    df = pr.trim_account_id_strings(df, ["finder", "onchainData_meta_who"])
    pr.print_long_df(df)
