import pandas as pd
from reports.quarterly_etl import QuarterlyReport, to_epoch, convert_timestamp
from defi.sources.statescan import StatescanExtractor


def get_metadata(network):
    data = StatescanExtractor(network).extract()
    start_block = data["assetHeight"]
    decimals = data["metadata"]["decimals"]

    return start_block, decimals


def get_data(network, start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    start = to_epoch(start)
    end = to_epoch(end)
    start_block, decimals = get_metadata(network)
    url_ending = "_" + str(start_block) + "/statistic"

    data = StatescanExtractor(network, url_ending=url_ending).extract(
        params={"from": start, "to": end})
    df = pd.json_normalize(data, sep="_")
    df["date"] = df["indexer_blockTime"].map(
        lambda t: convert_timestamp(t, "%Y-%m-%d", unit="ms"))
    df["transferAmount"] = (df["transferAmount_$numberDecimal"].astype(float)
                            / float("1e" + str(decimals)))
    df = df.reindex(columns=["date", "transferCount", "transferAmount",
                             "holderCount"])
    df = df.sort_values("date", ascending=False)

    return df


if __name__ == "__main__":
    stats = get_data("statemine")
    print(stats.to_string())
