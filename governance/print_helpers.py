import pandas as pd


def trim_long_strings(df, cols, new_len=50):
    new_df = df.copy()
    new_df[cols] = df[cols].applymap(
        lambda s: s[:new_len] + "..." if len(s) > 53 else s, na_action="ignore")

    return new_df


def trim_account_id_strings(df, cols):
    new_df = df.copy()
    new_df[cols] = df[cols].applymap(
        lambda s: s[:5] + "..." + s[-5:] if len(s) > 13 else s,
        na_action="ignore")

    return new_df


def print_long_df(df, head_len=10, tail_len=None):
    if tail_len is None:
        tail_len = head_len
    if df.shape[0] > head_len + tail_len:
        print(pd.concat([df.head(head_len), df.tail(tail_len)]).to_string())
    else:
        print(df.to_string())
