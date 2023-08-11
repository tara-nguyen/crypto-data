import pandas as pd
from governanceReport.sources.onchain import initialize_substrate as init


def get_data(network):
    data = init(network).get_constant("Referenda", "Tracks")
    data = [tup.value[0] for tup in data]

    return data


if __name__ == "__main__":
    tracks = get_data("kusama")
    print(tracks)
