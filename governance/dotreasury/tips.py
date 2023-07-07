from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["hash", "latestState_state", "medianValue", "fiatValue",
              "symbolPrice"]
    token_cols = ["medianValue"]

    data = Extractor("dotreasury", chain, "/tips").extract()
    df = Transformer(data).transform(fields, token_cols, chain, sort=False)

    return df


if __name__ == "__main__":
    tips = get_data("kusama")
    print_long_df(tips)
