from governance_quarterlyReport.sources.opensquare import *


def get_data(network):
    fields = ["proposalIndex", "value", "fiatValue"]
    data = OpensquareExtractor("dotreasury", network, "/proposals").extract()
    df = OpensquareTransformer(data).to_frame(fields, ["value"], network)

    return df


if __name__ == "__main__":
    proposals = get_data("kusama")
    print(proposals)
