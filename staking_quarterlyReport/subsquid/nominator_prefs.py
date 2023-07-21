from staking_quarterlyReport.subsquid import (nominations as nom,
                                              validator_stake as vs)
import pandas as pd


def get_data():
    # Validator stake and commission
    df_commission = pd.read_csv("validator_commission.csv")
    df_commission["era"] = df_commission["era"].astype(str)
    df_validators = vs.get_data().merge(df_commission)
    df_validators = df_validators.drop(columns="era")

    # Nominator stake and count
    df_nominations = nom.get_data()
    df_nominators = df_nominations.groupby(["date", "validator"])
    df_nominators = df_nominators.agg({"nominator": len})
    df_nominators.reset_index(inplace=True)

    df = df_nominators.merge(df_validators)
    df = df.rename(columns={"nominator": "nominatorCount",
                            "selfBonded": "validatorSelfStake"})
    df = df.drop(columns="validator")

    return df


if __name__ == "__main__":
    prefs = get_data()
    with pd.option_context("display.max_columns", None):
        print(prefs)
