from reports.quarterly_etl import extract
from time import sleep


class SubscanExtractor:
    def __init__(self, route, api_version=1, network="Polkadot", chain=None):
        self.method = "POST"

        if network in ["Polkadot", "Kusama"]:
            if chain is None:
                chain = network
        else:
            raise Exception('network must be either "Polkadot" or "Kusama" '
                            '(case-sensitive)')
        networks = SubscanNetworks().get_network(network)
        if chain in networks:
            self.url = f"https://{networks[chain][0]}.api.subscan.io/api/"
            self.url += "scan" if api_version == 1 else "v2/scan"
            self.url += route
        else:
            raise Exception(f"{chain} is not a parachain of {network}")

    def extract(self, payload):
        """Extract data from Subscan."""
        while True:
            try:
                data = extract(self.method, self.url, json=payload)["data"]
            except KeyError:
                sleep(0.1)
            else:
                data = data["list"]
                break

        return data


class SubscanNetworks:
    def __init__(self):
        self.main_parachains = ["polkadot", "kusama"]

    def get_network(self, relaychain):
        relaychain = relaychain.lower()
        if relaychain == "polkadot":
            networks = {"Polkadot": ["polkadot", 1e10],
                        "AssetHub-Polkadot": ["assethub-polkadot", 1e10],
                        "Acala": ["acala", 1e12],
                        "Astar": ["astar", 1e18],
                        "Bifrost": ["bifrost", 1e13],
                        "Centrifuge": ["centrifuge", 1e18],
                        "Clover Parachain": ["clv", 1e18],
                        "Composable": ["composable", 1e12],
                        "Crust Parachain": ["crust-parachain", 1e12],
                        "Darwinia2": ["darwinia", 1e18],
                        "Equilibrium": ["equilibrium", 1e9],
                        "HydraDX": ["hydradx", 1e12],
                        "Interlay": ["interlay", 1e10],
                        "KILT Spiritnet": ["spiritnet", 1e15],
                        "Manta": ["manta", 1e18],
                        "Moonbeam": ["moonbeam", 1e18],
                        "Nodle": ["nodle", 1e11],
                        "OriginTrail Parachain": ["origintrail", 1e12],
                        "Parallel": ["parallel", 1e12],
                        "Phala": ["phala", 1e12],
                        "Unique": ["unique", 1e18],
                        "Zeitgeist": ["zeitgeist", 1e10]}
        elif relaychain == "kusama":
            networks = {"Kusama": ["kusama", 1e12],
                        "AssetHub-Kusama": ["assethub-kusama", 1e12],
                        "Altair": ["altair", 1e18],
                        "Bajun": ["bajun", 1e12],
                        "Basilisk": ["basilisk", 1e12],
                        "Bifrost Kusama": ["bifrost-kusama", 1e12],
                        "Calamari": ["calamari", 1e12],
                        "Crab2": ["crab", 1e18],
                        "Datahighway Tanganika": ["datahighway", 1e18],
                        "Encointer": ["encointer", 1e12],
                        "Genshiro": ["genshiro", 1e9],
                        "Parallel Heiko": ["parallel-heiko", 1e12],
                        "IntegriTEE": ["integritee", 1e12],
                        "Karura": ["karura", 1e12],
                        "Khala": ["khala", 1e12],
                        "Kintsugi": ["kintsugi", 1e12],
                        "Mangata": ["mangatax", 1e18],
                        "Moonriver": ["moonriver", 1e18],
                        "Picasso": ["picasso", 1e12],
                        "Pioneer": ["pioneer", 1e18],
                        "Quartz": ["quartz", 1e18],
                        "Robonomics": ["robonomics", 1e18],
                        "Crust Shadow": ["shadow", 1e12],
                        "Shiden": ["shiden", 1e18],
                        "Turing": ["turing", 1e10]}
        else:
            raise Exception(
                f'The main parachain must be either {self.main_parachains[0]} '
                f'or {self.main_parachains[1]}')

        return networks
