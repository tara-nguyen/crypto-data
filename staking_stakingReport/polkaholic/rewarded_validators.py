from staking_stakingReport.sources.polkaholic import PolkaholicExtractor
from string import Template


def get_data():
    """Retrieve data on staking_stakingReport payouts from Polkaholic's dataset on Google Big
    Query and return a list of rewarded validators.
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
      AND ev.section = "staking_stakingReport"
      AND ev.method = "PayoutStarted"
    """)
    validators = PolkaholicExtractor().extract(query)["validator"].unique()

    return validators


if __name__ == "__main__":
    validators = get_data()
    print(len(validators), "validators")
    print(validators)
