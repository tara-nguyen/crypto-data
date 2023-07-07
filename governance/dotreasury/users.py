from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["address", "isProposer", "isBeneficiary", "isCouncilor",
              "proposals", "tips", "bounties", "childBounties"]

    data = Extractor("dotreasury", chain, "/participants").extract()
    df = Transformer(data).transform(fields, sort_by=fields[4:],
                                     ascending=False)

    return df


if __name__ == "__main__":
    users = get_data("kusama")
    print_long_df(users)
