import pandas as pd
from reports.quarterly_etl import extract
from time import sleep


class CoingeckoExtractor:
    """Extract data from CoinGecko API."""

    def __init__(self, route):
        self.method = "GET"
        self.url = f"https://api.coingecko.com/api/v3/coins{route}"

    def extract(self, params):
        while True:
            data = extract(self.method, self.url, params=params)
            if "status" in data:
                sleep(0.1)
            else:
                break

        return data


class CoingeckoTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data):
        self.data = data

    def to_frame(self, metric):
        df = pd.DataFrame(self.data[metric])
        df = df.set_axis(["timestamp", metric], axis=1)

        return df


class CoingeckoCoins:
    """Store the ids of coins available on CoinGecko."""

    def __init__(self):
        self.coins_dict = {
            "Polkadot": "polkadot",
            "Asset Hub-Polkadot": "polkadot",
            "Acala": "acala",
            "Astar": "astar",
            "Bifrost": "bifrost-native-coin",
            "Centrifuge": "centrifuge",
            "Clover Parachain": "clover-finance",
            "Crust Parachain": "crust-network",
            "Darwinia2": "darwinia-network-native-token",
            "Equilibrium": "equilibrium-token",
            "HydraDX": "hydradx",
            "Interlay": "interlay",
            "KILT Spiritnet": "kilt-protocol",
            "Moonbeam": "moonbeam",
            "Nodle": "nodle-network",
            "OriginTrail Parachain": "origintrail",
            "Parallel": "parallel-finance",
            "Phala": "pha",
            "Unique": "unique-network",
            "Zeitgeist": "zeitgeist",
            "Kusama": "kusama",
            "Asset Hub-Kusama": "kusama",
            "Altair": "altair",
            "Bajun": "ajuna-network",
            "Basilisk": "basilisk",
            "Bifrost Kusama": "bifrost-native-coin",
            "Calamari": "calamari-network",
            "Datahighway Tanganika": "datahighway",
            "Encointer": "kusama",
            "Genshiro": "genshiro",
            "IntegriTEE": "integritee",
            "Karura": "karura",
            "Khala": "pha",
            "Kintsugi": "kintsugi",
            "Moonriver": "moonriver",
            "Parallel Heiko": "parallel-finance",
            "Pioneer": "metaverse-network-pioneer",
            "Quartz": "quartz",
            "Robonomics": "robonomics-network",
            "Crust Shadow": "crust-storage-market",
            "Shiden": "shiden"}
        self.coins_list = [[network, cid]
                           for network, cid in self.coins_dict.items()]
        self.coin_ids = {cid for cid in self.coins_dict.values()}
