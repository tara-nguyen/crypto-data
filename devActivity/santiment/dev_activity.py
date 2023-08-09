from devActivity.sources.santiment import *


def get_data():
    data = SantimentExtractor().extract("dev_activity", moving_ave_base=7)
    df = SantimentTransformer(data).to_frame()
    df = df.set_axis(["date", "activity7dayAverage"], axis=1)

    return df


if __name__ == "__main__":
    activity = get_data()
    print(activity)
