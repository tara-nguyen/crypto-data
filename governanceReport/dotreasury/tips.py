from governanceReport.sources.opensquare import Extractor, Transformer
from governanceReport.print_helpers import print_long_df


def get_data(network):
    fields = ["hash", "latestState_state", "medianValue", "fiatValue",
              "symbolPrice"]
    token_cols = ["medianValue"]

    data = Extractor("dotreasury", network, "/tips").extract()
    df = Transformer(data).transform(fields, token_cols, network, sort=False)

    return df


if __name__ == "__main__":
    tips = get_data("kusama")
    print_long_df(tips)
