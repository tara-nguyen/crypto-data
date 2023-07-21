import pandas as pd
from reports.quarterly_etl import QuarterlyReport, to_epoch, extract


class MoonbeansExtractor:
    def __init__(self, graphql):
        if graphql:
            self.method = "POST"
            self.url = "https://graphql.moonbeans.io/graphql"
            self.headers = {"Content-Type": "application/json"}
        else:
            self.method = "GET"
            self.url = "https://api.moonbeans.io/collection"

    def extract_graphql(self, template, start=QuarterlyReport().start_time,
                        end=QuarterlyReport().end_time):
        """Extract data from Moonbeans graphql.

        Keyword arguments:
            start -- start point of the time range of interest
            end -- end point of the time range of interest
        """
        start, end = [to_epoch(t) for t in [start, end]]
        payload = template.substitute(start=start, end=end)
        data = extract(self.method, self.url, headers=self.headers,
                       data=payload)["data"]

        return data

    def extract(self, *args, **kwargs):
        if self.method == "POST":
            data = self.extract_graphql(*args, **kwargs)
        else:
            data = extract(self.method, self.url)

        return data


class MoonbeansTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self):
        """Convert json-encoded content to a dataframe."""
        df = pd.DataFrame(self.data)

        return df
