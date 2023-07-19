import ezsheets
import numpy as np
import pandas as pd
from requests import Session


class QuarterlyReport:
    def __init__(self):
        self.project_name = "QUARTERLY REPORT - 2023-Q2"
        self.start_time = pd.Timestamp(2023, 4, 1)
        self.end_time = pd.Timestamp(2023, 7, 1)
        self.networks = {"polkadot": 1e10, "kusama": 1e12}


class GoogleSheets:
    def __init__(self, spreadsheet=QuarterlyReport().project_name):
        self.spreadsheet = spreadsheet
        self.sheets = dict(prices_v_activity="devActivity_prices_v_activity",
                           dev_v_commits="devActivity_dev_v_commits",
                           developers="devActivity_developers",
                           fast_unstake="staking_fast_unstake",
                           rewards="staking_rewards",
                           nominator_prefs="staking_nominator_prefs")

    def load(self, sheet_key, df):
        """Load a dataframe to Google Sheets."""
        while True:
            load = input("Load to Google Sheets ([y]/n)? ").lower()
            if load in ["y", "n"]:
                break
            print("Invalid response")

        if load == "y":
            try:
                spr = ezsheets.Spreadsheet(self.spreadsheet)
            except ezsheets.EZSheetsException:
                spr = ezsheets.createSpreadsheet(self.spreadsheet)

            sheet = self.sheets[sheet_key]
            if sheet in spr.sheetTitles:
                sh = spr.__getitem__(sheet)
                print(f"Clearing '{sheet}' contents...")
                sh.clear()
            else:
                print(f"Creating new sheet '{sheet}'...")
                sh = spr.createSheet(sheet)
            print("Updating sheet...")
            sh.updateRows([df.columns.tolist()] + df.values.tolist())
            print(f"Sheet '{sheet}' updated.")


def extract(method, url, **kwargs):
    """Query from an online source and return the json-encoded content of the
     response.
     """
    resp = Session().request(method, url, **kwargs)
    # if not resp.ok:
    #     print("Error", resp.status_code)
    data = resp.json()

    return data


def to_epoch(time):
    """Convert a time string to epoch time (i.e. the number of seconds from
    1970-01-01 00:00:00.
    """
    time = pd.Timestamp(time) - pd.Timestamp(0)
    time = int(np.ceil(time.total_seconds()))

    return time


def get_token_amount(x, network):
    """Convert a hex string or a character string containing only digits to an
    integer. Then, divide the integer by the denomination of the given network's
    token.
    """
    if isinstance(x, str):
        if x.isdigit() or "." in x:
            x = float(x)
        else:
            x = int(x, 16)

    networks = QuarterlyReport().networks
    if network in networks:
        x /= networks[network]
    else:
        raise Exception(
            f'network must be either "{networks[0]}" or "{networks[1]}"')

    return x


def print_and_load(title, df, sheet_key):
    print()
    print(f"-----{title}-----")
    print(df)
    print()
    GoogleSheets().load(sheet_key, df)