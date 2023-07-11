from devActivity.sources.tokenterminal import (TokenTerminalExtractor,
                                               TokenTerminalTransformer)


def get_data():
    data = TokenTerminalExtractor("active_developers").extract()
    df = TokenTerminalTransformer(data).to_frame()

    return df


if __name__ == "__main__":
    dev = get_data()
    print(dev)
