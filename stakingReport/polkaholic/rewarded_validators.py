from stakingReport.sources.polkaholic import *
from string import Template


def get_raw_data(path):
    """Retrieve data on staking payouts from Polkaholic's Big Query dataset,
    save the dataset to a csv file, and return a dataframe.
    """
    query = Template("""
    SELECT 
      DATE(ex.block_time) date,
      JSON_VALUE(data[1]) validator
    FROM substrate-etl.crypto_polkadot.events0 ev
    JOIN (SELECT * FROM substrate-etl.crypto_polkadot.extrinsics0) ex
    ON ev.extrinsic_id = ex.extrinsic_id
    WHERE
      ex.block_time >= "$start"
      AND ex.block_time < "$end"
      AND ev.section = "staking"
      AND ev.method = "PayoutStarted"
    """)
    data = PolkaholicExtractor().extract(query)
    df = PolkaholicTransformer(data).to_frame()
    df.to_csv(path, index=False)

    return df


def get_data(path="data_raw/rewarded_validators_raw.csv"):
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        df = get_raw_data(path)

    validators = df["validator"].unique()

    return validators


if __name__ == "__main__":
    validators = get_data()
    print(len(validators), "validators")
    print(validators)
