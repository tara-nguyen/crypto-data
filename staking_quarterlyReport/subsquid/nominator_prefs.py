import pandas as pd
from staking_quarterlyReport.subsquid import (nominations as nom,
                                              validator_stake as vs)


def get_data(path="validator_commission.csv"):
    # Validator stake and commission
    df_commission = pd.read_csv(path)
    df_commission["era"] = df_commission["era"].astype(str)
    df_validators = vs.get_data().merge(df_commission)
    df_validators = df_validators.drop(columns="era")

    # Nominator stake and count
    df_nominations = nom.get_data()
    df_nominators = df_nominations.groupby(["date", "validator"])
    df_nominators = df_nominators.agg({"nominator": len})
    df_nominators.reset_index(inplace=True)

    df = df_validators.merge(df_nominators)
    df = df.reindex(columns=["date", "nominator", "commission", "selfBonded"])
    df = df.rename(columns={"nominator": "nominatorCount",
                            "selfBonded": "validatorSelfStake"})

    return df


if __name__ == "__main__":
    prefs = get_data()
    with pd.option_context("display.max_columns", None):
        print(prefs)
