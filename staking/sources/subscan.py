from staking import extract
from time import sleep


class SubscanExtractor:
    def __init__(self, path_end, api_version=1):
        self.method = "POST"
        self.url = "https://polkadot.api.subscan.io/api/"
        self.url += "scan" if api_version == 1 else "v2/scan"
        self.url += path_end

    def extract(self, payload):
        """Extract data from Subscan."""
        while True:
            try:
                data = extract(self.method, self.url, json=payload)["data"]
            except KeyError:
                sleep(0.1)
            else:
                break

        return data
