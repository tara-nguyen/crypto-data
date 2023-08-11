from substrateinterface import SubstrateInterface


def initialize_substrate(network):
    network = network.lower()
    if network == "polkadot":
        url = "wss://rpc.polkadot.io"
        ss58_format = 0
    elif network == "kusama":
        url = "wss://kusama-rpc.polkadot.io/"
        ss58_format = 2
    else:
        raise Exception('network must be either "polkadot" or "kusama"')

    substrate = SubstrateInterface(url=url, ss58_format=ss58_format,
                                   type_registry_preset=network)

    return substrate
