from defillama import parachain_tvl as prc, protocol_tvl as ptc
from hydradx import tvl as hdx
from parity import usdt_supply as sup
from statescan import usdt
from reports.quarterly_etl import QuarterlyReport, print_long_df


def main():
    df_parachains = prc.get_data()
    print("DeFi Parachains")
    print_long_df(df_parachains)

    for network in QuarterlyReport().networks:
        df_dex = ptc.get_data("Dexes", network)

        if network == "polkadot":
            df_hydradx = hdx.get_data("hydradx/hydradx_tvl_volume_q1q2.csv")
            df_dex = df_dex.merge(df_hydradx, on="date")

            df_chains_sorted = df_dex.drop(columns=["date", "Others"]).head(1).T
            df_chains_sorted = df_chains_sorted.sort_values(0, ascending=False)

            columns = ["date"] + df_chains_sorted.index.tolist() + ["Others"]
            df_dex = df_dex.reindex(columns=columns)
        print(f"\nTop DEX Protocols on {network.title()}")
        print_long_df(df_dex)

    df_lending = ptc.get_data("Lending")
    print("\nTop Lending Protocols")
    print_long_df(df_lending)

    df_lqs = ptc.get_data("Liquid Staking")
    print("\nTop Liquid Staking Protocols")
    print_long_df(df_lqs)

    df_usdt_supply = sup.get_data("parity/usdt_supply.csv")
    print("\nUSDT Supply")
    print_long_df(df_usdt_supply)

    for chain in ["statemint", "statemine"]:
        chain_name = ("Asset Hub-Polkadot"
                      if chain == "statemint" else "Asset Hub-Kusama")
        network = chain_name.split("-")[1].lower()

        df_usdt = usdt.get_data(chain)
        print(f"\nUSDT on {chain_name}")
        print_long_df(df_usdt)


if __name__ == "__main__":
    main()
