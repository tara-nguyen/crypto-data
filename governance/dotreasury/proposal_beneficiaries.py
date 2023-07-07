from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["beneficiary", "count", "value", "fiatValue"]
    token_cols = ["value"]

    data = Extractor("dotreasury", chain, "/proposals/beneficiaries").extract()
    df = Transformer(data).transform(fields, token_cols, chain,
                                     sort_by=["value", "fiatValue"],
                                     ascending=False)

    return df


if __name__ == "__main__":
    beneficiaries = get_data("kusama")
    print_long_df(beneficiaries)
