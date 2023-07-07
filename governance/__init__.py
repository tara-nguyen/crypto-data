import pandas as pd


class GovernanceReport:
    def __init__(self):
        self.project_name = "STAKING REPORT - 2023-JUNE"
        self.timestamp_format = "%Y-%m-%d %H:%M:%S"
        self.timestamp = pd.Timestamp.now("utc").strftime(self.timestamp_format)
        self.chains = {"polkadot": 1e10, "kusama": 1e12}


def get_token_amount(x, chain):
    """Convert a hex string or a character string containing only digits to an
    integer. Then, divide the integer by the denomination of the given chain's
    token.
    """
    if isinstance(x, str):
        if x.isdigit() or "." in x:
            x = float(x)
        else:
            x = int(x, 16)

    chains = GovernanceReport().chains
    if chain in chains:
        x /= chains[chain]
    else:
        raise Exception(f'chain must be either "{chains[0]}" or "{chains[1]}"')

    return x


def convert_timestamp(t):
    new_t = pd.to_datetime(t,
                           unit="ms" if isinstance(t, (int, float)) else None)
    new_t = new_t.strftime(GovernanceReport().timestamp_format)

    return new_t
