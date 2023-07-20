import pandas as pd
from substrateinterface import SubstrateInterface


class PolkadotjsExtractor:
    def __init__(self):
        self.substrate = SubstrateInterface(
            url="wss://rpc.polkadot.io", ss58_format=0,
            type_registry_preset="polkadot")
        self.history_depth = self.substrate.get_constant(
            "Staking", "HistoryDepth").value + 1

    def extract(self, *args):
        data = self.substrate.query_map(*args)

        return data


class PolkadotjsTransformer:
    def __init__(self, data):
        self.data = data
        self.df = None

    def to_frame(self, **kwargs):
        self.df = pd.DataFrame([[d.value for d in dat] for dat in self.data],
                               **kwargs)

        return self
