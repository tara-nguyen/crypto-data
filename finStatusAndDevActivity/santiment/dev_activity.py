from finStatusAndDevActivity.sources.santiment import (SantimentExtractor,
                                                       SantimentTransformer)


def get_data():
    data = SantimentExtractor().extract("dev_activity", moving_ave_base=7)
    df = SantimentTransformer(data).to_frame()

    return df


if __name__ == "__main__":
    activity = get_data()
    print(activity)
