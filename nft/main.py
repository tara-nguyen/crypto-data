import moonbeans.sales as ms
import nftrade.sales as ns
import rmrk.sales as rs
import tofunft.sales as ts
import pandas as pd
from reports.quarterly_etl import print_and_load as pl


def main():
    path = "coingecko/prices_raw.csv"
    df_moonbeans = ms.get_data(path)
    df_nftrade = ns.get_data(path)
    df_rmrk = rs.get_data(path, "rmrk/Sales Volume.csv")
    df_tofunft = ts.get_data(path, "tofunft/sales.csv")
    df = pd.concat([df_moonbeans, df_nftrade, df_rmrk, df_tofunft])

    sources = ["moonbeans"] * df_moonbeans.shape[0]
    sources += ["nftrade"] * df_nftrade.shape[0]
    sources += ["rmrk"] * df_rmrk.shape[0]
    sources += ["tofunft"] * df_tofunft.shape[0]
    df["source"] = sources
    df["market_chain"] = df.eval("source.str.cat(chain, '_')")

    df = df.pivot(index="date", columns="market_chain", values="totalUSD")
    df.insert(0, "rmrk_kusama", df.pop("rmrk_kusama"))
    df = df.sort_index(ascending=False).reset_index()

    pl("NFT Marketplace", df, "nft_marketplace")


if __name__ == "__main__":
    main()
