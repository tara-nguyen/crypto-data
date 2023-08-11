import pandas as pd
from reports.quarterly_etl import QuarterlyReport, to_epoch
from sources.statescan import StatescanExtractor


def get_metadata(chain):
    data = StatescanExtractor(chain).extract()
    start_block = data["assetHeight"]
    decimals = data["metadata"]["decimals"]

    return start_block, decimals


def get_data(chain, start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    start, end = [to_epoch(t) for t in [start, end]]
    start_block, decimals = get_metadata(chain)
    url_ending = "_" + str(start_block) + "/statistic"

    data = StatescanExtractor(chain, url_ending=url_ending).extract(
        params={"from": start, "to": end})
    df = pd.json_normalize(data, sep="_")
    df["date"] = pd.to_datetime(df["indexer_blockTime"],
                                unit="ms").dt.strftime("%Y-%m-%d")
    df["transferAmount"] = (df["transferAmount_$numberDecimal"].astype(float)
                            / float("1e" + str(decimals)))
    df = df.reindex(columns=["date", "transferCount", "transferAmount",
                             "holderCount"])
    df = df.sort_values("date", ascending=False)

    return df


if __name__ == "__main__":
    stats = get_data("statemine")
    print(stats)
