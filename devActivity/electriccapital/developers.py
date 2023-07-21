import pandas as pd
from devActivity.sources.electriccapital import (ElectricCapitalExtractor,
                                                 ElectricCapitalTransformer)


def get_data(start=pd.Timestamp(2023, 1, 1)):
    data = ElectricCapitalExtractor().extract()
    df = ElectricCapitalTransformer(data).to_frame(
        start=start, new_cols=["fulltime", "parttime", "onetime"])
    df = df.eval("fulltimeRatio = fulltime / (fulltime + parttime + onetime)")

    return df


if __name__ == "__main__":
    dev = get_data()
    print(dev)
