import pandas as pd
from subsquare import opengov_referenda as ref
from reports.quarterly_etl import QuarterlyReport, print_long_df


def main():
    opengov_support = []
    networks = list(QuarterlyReport().networks.keys())

    for network in networks:
        df_refs = ref.get_data(network)
        print(f"\n{network.title()} OpenGov Referenda")
        print_long_df(df_refs["counts"])

        print(f"\nApproved {network.title()} OpenGov Spend")
        print_long_df(df_refs["spend"])

        opengov_support.append(df_refs["support"])

    df_turnout = pd.merge(
        opengov_support[0], opengov_support[1], "outer", on="track",
        suffixes=(f"_{networks[0]}", f"_{networks[1]}")).fillna(0)
    df_turnout = df_turnout.set_index("track")
    df_turnout = df_turnout.reindex(index=opengov_support[1]["track"])
    df_turnout.reset_index(inplace=True)
    print("\nOpenGov Turnout")
    print_long_df(df_turnout)


if __name__ == "__main__":
    main()
