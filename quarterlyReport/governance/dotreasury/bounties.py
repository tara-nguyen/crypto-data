from quarterlyReport.governance.sources.opensquare import OpensquareExtractor
from quarterlyReport.governance.sources.opensquare import OpensquareTransformer


def get_data(network):
    fields = ["bountyIndex", "value", "fiatValue"]
    data = OpensquareExtractor("dotreasury", network, "/bounties").extract()
    df = OpensquareTransformer(data).to_frame(fields, ["value"], network)

    return df


if __name__ == "__main__":
    bounties = get_data("kusama")
    print(bounties)