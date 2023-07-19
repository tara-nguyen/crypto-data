import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract
from string import Template


class SantimentExtractor:
    def __init__(self):
        self.method = "POST"
        self.url = "https://api.santiment.net/graphql"
        self.headers = {"Content-Type": "application/json"}
        self.timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
        self.template = Template(
            '{"query": "{getMetric(metric: \\"$metric\\") {timeseriesData('
            'slug: \\"$slug\\" from: \\"$start\\" to: \\"$end\\" transform: '
            '{type: \\"moving_average\\", movingAverageBase: $moving_ave_base})'
            '{datetime value}}}"}')

    def extract(self, metric, slug="polkadot-new",
                start=QuarterlyReport().start_time,
                end=QuarterlyReport().end_time, moving_ave_base=1):
        start, end = [t.strftime(self.timestamp_format) for t in [start, end]]
        payload = self.template.substitute(
            metric=metric, slug=slug, start=start, end=end,
            moving_ave_base=moving_ave_base)
        data = extract(self.method, self.url, headers=self.headers,
                       data=payload)["data"]["getMetric"]["timeseriesData"]

        return data


class SantimentTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self):
        """Convert json-encoded content to a dataframe."""
        df = pd.DataFrame(self.data)
        df["date"] = df["datetime"].str.slice(stop=10)
        df = df.reindex(columns=["date", "value"])
        df = df.sort_values("date", ascending=False)

        return df
