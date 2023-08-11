from tokenguard import wasm
from reports.quarterly_etl import print_long_df


def main():
    for network in ["alephzero", "astar"]:
        df_wasm = wasm.get_data(network, file_path_prefix="tokenguard/")
        print(f"WASM Contracts on {network.title()}")
        print_long_df(df_wasm)


if __name__ == "__main__":
    main()
