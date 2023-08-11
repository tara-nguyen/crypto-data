from tokenguard import wasm
from quarterly_etl import print_and_load as pl


def main():
    for network in ["alephzero", "astar"]:
        df_wasm = wasm.get_data(network, file_path_prefix="tokenguard/")
        pl(f"WASM Contracts on {network.title()}", df_wasm, f"wasm_{network}")


if __name__ == "__main__":
    main()
