import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from devActivity.sources.tokenterminal import (TokenTerminalExtractor,
                                               TokenTerminalTransformer)


def get_data():
    data = TokenTerminalExtractor("code_commits").extract()

    trf = TokenTerminalTransformer(data)
    start = QuarterlyReport().start_time - pd.Timedelta(days=29)
    df = trf.to_frame(start=start).sort_values("date")
    df = df.rolling(30, on="date").sum().dropna()
    df = df.sort_values("date", ascending=False)

    return df


if __name__ == "__main__":
    commits = get_data()
    print(commits)
