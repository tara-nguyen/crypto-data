from governanceReport.sources.opensquare import Extractor, Transformer
from governanceReport.print_helpers import print_long_df


def get_data(network):
    fields = ["beneficiary", "count", "value", "fiatValue"]
    token_cols = ["value"]

    data = Extractor("dotreasury", network,
                     "/proposals/beneficiaries").extract()
    df = Transformer(data).transform(fields, token_cols, network,
                                     sort_by=["value", "fiatValue"],
                                     ascending=False)

    return df


if __name__ == "__main__":
    beneficiaries = get_data("kusama")
    print_long_df(beneficiaries)
