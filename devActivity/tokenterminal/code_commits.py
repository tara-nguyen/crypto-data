import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from devActivity.sources.tokenterminal import (TokenTerminalExtractor,
                                               TokenTerminalTransformer)


def get_data():
    data = TokenTerminalExtractor("code_commits").extract()

    trf = TokenTerminalTransformer(data)
    start = QuarterlyReport().start_time - pd.Timedelta(days=29)
    df = trf.to_frame(start=start).sort_values("date")
    df = df.rolling(30, on="date").mean().dropna()
    df = df.rename(columns={"value": "codeCommits30dayAverage"})

    return df


if __name__ == "__main__":
    commits = get_data()
    print(commits)
