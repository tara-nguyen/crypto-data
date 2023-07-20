import pandas as pd
from reports.staking_etl import (StakingReport, to_epoch, get_time,
                                 get_daily_data)
from staking_stakingReport.sources.coingecko import CoingeckoExtractor, CoingeckoTransformer


def get_data(start=StakingReport().start_era, end=StakingReport().end_era):
    """Retrieve price data from CoinGecko and return a dataframe."""
    metric = "prices"
    limit = 90
    # If the time range is above 90 days, the data that the CoinGecko API
    # returns will be daily data (00:00 UTC). To obtain hourly data, divide the
    # time range into 90-day-long intervals.
    if end - start > 90:
        intervals = range(start, end + 1, limit)
        if list(intervals)[-1] < end:
            intervals = list(intervals) + [end]
    else:
        intervals = [start, end]
    intervals = [to_epoch(get_time(interval)) for interval in intervals]
    df = pd.DataFrame([])

    extractor = CoingeckoExtractor("/market_chart/range")
    for i in range(len(intervals)-1):
        querystring = {"vs_currency": "usd", "from": f"{intervals[i]}",
                       "to": f"{intervals[i+1] - 1}"}
        data = extractor.extract(querystring)
        transformer = CoingeckoTransformer(data).to_frame(metric)
        df = pd.concat([df, transformer.df])

    df = get_daily_data(df, True).rename(columns={metric: "price_USD"})

    return df


if __name__ == "__main__":
    prices = get_data()
    print(prices)
