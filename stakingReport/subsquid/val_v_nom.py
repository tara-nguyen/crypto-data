from stakingReport.sources.subsquid import (SubsquidExtractor,
                                            SubsquidTransformer)
from string import Template


def get_data():
    """Retrieve from Subsquid data on staker counts and stake by staker type and
    return a dataframe.
    """
    metric = "hourlyCharts"
    template = Template(
        '{"query": "query MyQuery {$metric(where: {timestamp_gte:\\"$start\\", '
        'timestamp_lt:\\"$end\\"}) {timestamp stakingValidatorsAmount '
        'stakingNominatorsActiveAmount stakingNominatorsInactiveAmount '
        'stakingTotalStakeValidators stakingTotalStakeNominatorsActive '
        'stakingMinActiveNominatorStake}}", "operationName": "MyQuery"}')

    extractor = SubsquidExtractor("gs_stats")
    data = extractor.extract(template, metric)

    transformer = SubsquidTransformer(data).to_frame().get_daily_data()
    transformer.transform_numeric(
        ["stakingTotalStakeValidators", "stakingTotalStakeNominatorsActive",
         "stakingMinActiveNominatorStake"])
    df = transformer.df.set_axis(
        ["date", "totalValidators", "activeNominators", "inactiveNominators",
         "validatorStake", "nominatorStake", "minActiveStake"], axis=1)

    # Validators versus nominators
    df = df.eval(
        """totalNominators = activeNominators + inactiveNominators
        validatorStakeRatio = validatorStake / (validatorStake + nominatorStake)
        """)
    df = df.reindex(
        columns=["date", "totalValidators", "totalNominators", "validatorStake",
                 "nominatorStake", "validatorStakeRatio", "minActiveStake"])

    return df


if __name__ == "__main__":
    val_v_nom = get_data()
    # print(df)
    val_v_nom = val_v_nom.query("date > '2023-05-15'").astype(str)
    print(val_v_nom.to_string())
