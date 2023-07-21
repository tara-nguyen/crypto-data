import json
import re
import pandas as pd
import matplotlib.pyplot as plt
from reports.staking_etl import (StakingReport, extract, get_time,
                                 transform_numeric, get_daily_data)
from itertools import product


class SubsquidExtractor:
    def __init__(self, path_name):
        self.method = "POST"
        paths = dict(gs_stats="/gs-stats-polkadot/graphql",
                     explorer="/polkadot-explorer/graphql")
        self.url = "https://squid.subsquid.io" + paths[path_name]
        self.headers = {"Content-Type": "application/json"}

    def extract(self, template, metric, daily=False,
                start=StakingReport().start_time, end=StakingReport().end_time,
                **kwargs):
        """Extract data from Subsquid.

        Keyword arguments:
            daily -- whether data should be extracted by date instead of all
            at once
            start -- start point of the time range of interest
            end -- end point of the time range of interest
        """
        data_all = []

        if daily:
            if end > StakingReport().end_time:
                end = StakingReport().end_time
            limit = 10000
            i = 0
            while True:
                try:
                    payload = template.substitute(
                        metric=metric, start=start, end=end, limit=limit,
                        offset=limit*i, **kwargs)
                    data = extract(self.method, self.url, headers=self.headers,
                                   data=payload)
                except json.JSONDecodeError:
                    continue
                data = data["data"][metric]
                data_all += data
                if len(data) < limit:
                    break
                i += 1
        else:
            payload = template.substitute(metric=metric, start=start, end=end,
                                          **kwargs)
            while True:
                try:
                    data_all = extract(self.method, self.url,
                                       headers=self.headers, data=payload)
                except json.JSONDecodeError:
                    continue
                data_all = data_all["data"][metric]
                break

        return data_all


class SubsquidTransformer:
    def __init__(self, data_all, chunked=False):
        """Initialize the transformer.

        Keyword arguments:
            chunked -- if this is `True`, `data_all` must be a list of lists,
            otherwise `data_all` is a flat list.
        """
        self.data = data_all if not chunked else sum(data_all, [])
        self.df = None

    def to_frame(self, *args, nested=False):
        """Convert json-encoded content to a dataframe."""
        if nested:
            self.df = pd.json_normalize(self.data, *args)
        else:
            self.df = pd.DataFrame(self.data)

        return self

    def get_daily_data(self, **kwargs):
        """Filter a dataframe to retain only the last data point from each
         date.
         """
        self.df = get_daily_data(self.df, False, **kwargs)

        return self

    def transform_numeric(self, cols, *args):
        """Convert string columns to float type and, if `*args` are given,
        divide the values by the specified divisor(s).
        """
        self.df[cols] = transform_numeric(self.df[cols].astype(float), *args)

        return self

    def split_id_string(self, staker_types=None, to_frame=True, drop_eras=True):
        """Split each value in a column named 'id' into two or three separate
        values. If `to_frame` is set to `True`, turn each collection of new
        values into a column of the class's dataframe and return that dataframe,
        otherwise simply return the collections.
        """
        parts = self.df["id"].map(lambda s: s.split("-"))
        parts = [parts.map(lambda l: l[i]) for i in range(len(parts.iloc[0]))]
        if to_frame:
            parts = pd.concat(parts, axis=1)
            parts = parts.set_axis(["era"] + staker_types, axis=1)
            parts["date"] = get_time(parts["era"].astype(int), "%Y-%m-%d")
            if drop_eras:
                parts = parts.drop(columns="era")

        return parts


class SubsquidSnapshot:
    def __init__(self, df, col):
        self.target_col = df[col] / 1e6
        self.bins = pd.Series([None] * self.target_col.size,
                              index=self.target_col.index)

    def plot(self):
        """Make a box plot of the target column."""
        self.target_col.plot.box(sym=".")
        plt.show()

    def get_histogram_bins(self, threshold, num_bins_no_outliers=5):
        """Create bins based on the values of the target column."""
        outlier_ids = self.target_col[self.target_col > threshold].index
        df_no_outliers = self.target_col.drop(index=outlier_ids)
        bins_no_outliers = pd.cut(df_no_outliers, num_bins_no_outliers,
                                  right=False, precision=2)
        outlier_cat = f"> {bins_no_outliers.max().right:.2f}"
        bins_no_outliers = bins_no_outliers.cat.rename_categories(
            lambda c: f"{c.left:.2f}-{c.right:.2f}")

        bins_with_outlier_cat = bins_no_outliers.cat.add_categories(outlier_cat)
        self.bins = pd.concat([self.bins, bins_with_outlier_cat], axis=1)
        self.bins = self.bins.iloc[:, 1].fillna(outlier_cat)

        return self.bins


class StakeChangesByDate:
    def __init__(self, df, variables):
        self.df = df
        self.variables = variables

    def get_colnames(self, suffixes):
        """Convert a MultiIndex into a simple Index."""
        columns = ["_".join(tup) for tup in product(self.variables, suffixes)]

        return columns

    def get_data(self, dates):
        """Transform the current dataframe to show the stake amounts on each
        date and the changes from one day to the next.
        """
        df = self.df.query("date in @dates").copy()
        df = df.pivot(index="validator", columns="date").fillna(0)
        df = df.set_axis(df.columns.map("_".join), axis=1)

        df2 = df.diff(axis=1).reindex(columns=self.get_colnames(dates[1:]))
        df = df.join(df2, rsuffix="_Change")

        # Filter and rename columns
        num_dates = len(dates)
        if num_dates == 2:
            df.columns = [re.sub(r"_.*(?=Change)", "", i) for i in df.columns]
        col_group1 = df.columns[:num_dates]
        col_group2 = df.columns[(num_dates * 2):(-num_dates + 1)]
        col_group3 = df.columns[num_dates:(num_dates * 2)]
        col_group4 = df.columns[(-num_dates + 1):]
        columns = [col.to_list()
                   for col in [col_group1, col_group2, col_group3, col_group4]]
        columns = sum(columns, [])

        df = df.reindex(columns=columns).reset_index()
        df = df.sort_values(df.columns[1:3].to_list(), ascending=False)

        return df
