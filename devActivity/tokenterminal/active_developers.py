from devActivity.sources.tokenterminal import *


def get_data():
    data = TokenTerminalExtractor("active_developers").extract()
    df = TokenTerminalTransformer(data).to_frame()
    df = df.rename(columns={"value": "activeDevs"})

    return df


if __name__ == "__main__":
    dev = get_data()
    print(dev)
