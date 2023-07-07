import print_helpers as pr
from governance.sources.opensquare import Extractor, Transformer
from string import Template
from concurrent.futures import ThreadPoolExecutor


def get_referenda(chain):
    fields = ["referendumIndex", "track", "state_name", "proposer", "title",
              "content", "createdAt", "onchainData_info_deciding_since",
              "onchainData_lastConfirmStartedAt_blockHeight",
              "onchainData_lastConfirmStartedAt_blockTime",
              "onchainData_info_deciding_confirming",
              "onchainData_tally_bareAyes", "onchainData_tally_ayes",
              "onchainData_tally_nays", "onchainData_tally_electorate",
              "onchainData_info_enactment_after",
              "onchainData_info_enactment_at", "onchainData_enactment_when",
              "state_args_result_ok", "state_indexer_blockTime",
              "commentsCount", "polkassemblyCommentsCount",
              "onchainData_openGovReferenda"]
    time_cols = ["onchainData_lastConfirmStartedAt_blockTime",
                 "state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", chain, "/fellowship/referenda").extract()
    df = Transformer(data).transform(fields, chain=chain, time_cols=time_cols)

    return df


def get_referendum_votes(chain):
    df_referenda = get_referenda(chain)
    ref_ids = df_referenda["referendumIndex"]

    fields = ["referendumIndex", "voter", "indexer_blockTime", "isAye", "votes",
              "tally_bareAyes", "tally_ayes", "tally_nays",
              "indexer_blockHeight", "indexer_extrinsicIndex",
              "indexer_eventIndex"]
    time_cols = ["indexer_blockTime"]
    route_template = Template("/fellowship/referenda/$ref_id/vote-calls")
    routes = [route_template.substitute(ref_id=rid) for rid in ref_ids]

    with ThreadPoolExecutor() as exe:
        futures = [exe.submit(Extractor("subsquare", chain, route).extract)
                   for route in routes]
    data = sum([future.result() for future in futures], [])
    df_votes = Transformer(data).transform(
        fields, time_cols=time_cols,
        sort_by=["indexer_blockTime", "referendumIndex"], ascending=False)
    df = {"referenda": df_referenda, "votes": df_votes}

    return df


def get_data(chain):
    df = get_referendum_votes(chain)

    return df


if __name__ == "__main__":
    ref = get_data("kusama")
    df = pr.trim_long_strings(ref["referenda"], ["title", "content"])
    df = pr.trim_account_id_strings(df, ["proposer"])
    pr.print_long_df(df)
    print()
    df = pr.trim_account_id_strings(ref["votes"], ["voter"])
    pr.print_long_df(df)
