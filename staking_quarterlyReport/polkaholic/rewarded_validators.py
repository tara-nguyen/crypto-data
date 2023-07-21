from staking_quarterlyReport.sources.polkaholic import (PolkaholicExtractor,
                                                        PolkaholicTransformer)
from string import Template


def get_data():
    """Retrieve data on staking payouts from Polkaholic's dataset on Google Big
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
      AND ev.section = "staking"
      AND ev.method = "PayoutStarted"
    """)
    data = PolkaholicExtractor().extract(query)
    validators = PolkaholicTransformer(data).to_frame()["validator"].unique()

    return validators


if __name__ == "__main__":
    validators = get_data()
    print(len(validators), "validators")
    print(validators)
