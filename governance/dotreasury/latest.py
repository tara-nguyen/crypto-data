import pandas as pd
from governance import GovernanceReport
from governance.sources.opensquare import Extractor, Transformer


def get_data():
    fields = ["income_inflation", "income_transfer", "income_slash",
              "income_slashSeats_treasury", "income_slashSeats_staking",
              "income_slashSeats_democracy", "income_slashSeats_election",
              "income_slashSeats_identity", "income_slashSeats_referenda",
              "income_slashSeats_fellowshipRefereda", "income_others",
              "count_referenda_all", "count_referenda_unFinished",
              "count_proposal_all", "count_proposal_unFinished",
              "count_proposal_openGov", "count_tip_all", "count_tip_unFinished",
              "count_bounty_all", "count_bounty_unFinished",
              "count_childBounty_all", "count_childBounty_unFinished",
              "count_burnt_all", "output_referendaSpent_treasurer_value",
              "output_referendaSpent_small_tipper_value",
              "output_referendaSpent_big_tipper_value",
              "output_referendaSpent_small_spender_value",
              "output_referendaSpent_medium_spender_value",
              "output_referendaSpent_big_spender_value",
              "output_referendaSpent_treasurer_fiatValue",
              "output_referendaSpent_small_tipper_fiatValue",
              "output_referendaSpent_big_tipper_fiatValue",
              "output_referendaSpent_small_spender_fiatValue",
              "output_referendaSpent_medium_spender_fiatValue",
              "output_referendaSpent_big_spender_fiatValue",
              "output_referendaSpent_treasurer_count",
              "output_referendaSpent_small_tipper_count",
              "output_referendaSpent_big_tipper_count",
              "output_referendaSpent_small_spender_count",
              "output_referendaSpent_medium_spender_count",
              "output_referendaSpent_big_spender_count",
              "output_proposal_value", "output_proposal_fiatValue",
              "output_tip_value", "output_tip_fiatValue", "output_bounty_value",
              "output_bounty_fiatValue", "output_burnt_value",
              "output_transfer_value", "toBeAwarded_total", "latestSymbolPrice"]
    token_cols = ["income_inflation", "income_transfer", "income_slash",
                  "income_slashSeats_treasury", "income_slashSeats_staking",
                  "income_slashSeats_democracy", "income_slashSeats_election",
                  "income_slashSeats_identity", "income_slashSeats_referenda",
                  "income_slashSeats_fellowshipRefereda", "income_others",
                  "output_referendaSpent_treasurer_value",
                  "output_referendaSpent_small_tipper_value",
                  "output_referendaSpent_big_tipper_value",
                  "output_referendaSpent_small_spender_value",
                  "output_referendaSpent_medium_spender_value",
                  "output_referendaSpent_big_spender_value",
                  "output_proposal_value", "output_tip_value",
                  "output_bounty_value", "output_burnt_value",
                  "output_transfer_value", "toBeAwarded_total"]

    df = pd.DataFrame([])
    for chain in GovernanceReport().chains:
        data = Extractor("dotreasury", chain, "/overview").extract()
        df_chain = Transformer(data).transform(fields, token_cols, chain)
        df_chain["chain"] = chain
        df = pd.concat([df, df_chain], ignore_index=True)
    df = df.reindex(columns=["chain"] + fields)

    return df


if __name__ == "__main__":
    stats = get_data()
    print(stats.to_string())
