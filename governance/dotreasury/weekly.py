from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(network):
    fields = ["indexer_blockHeight", "indexer_blockTime", "treasuryBalance",
              "income_transfer", "income_slash", "income_slashSeats_treasury",
              "income_slashSeats_staking", "income_slashSeats_democracy",
              "income_slashSeats_election", "income_slashSeats_identity",
              "income_slashSeats_referenda",
              "income_slashSeats_fellowshipRefereda", "income_others",
              "output_proposal", "output_tip", "output_bounty", "output_burnt"]
    token_cols = ["treasuryBalance", "income_transfer", "income_slash",
                  "income_slashSeats_treasury", "income_slashSeats_staking",
                  "income_slashSeats_democracy", "income_slashSeats_election",
                  "income_slashSeats_identity", "income_slashSeats_referenda",
                  "income_slashSeats_fellowshipRefereda", "income_others",
                  "output_proposal", "output_tip", "output_bounty",
                  "output_burnt"]
    time_cols = ["indexer_blockTime"]

    data = Extractor("dotreasury", network, "/stats/weekly").extract()
    df = Transformer(data).transform(fields, token_cols, network, time_cols)

    return df


if __name__ == "__main__":
    stats = get_data("kusama")
    print_long_df(stats)
