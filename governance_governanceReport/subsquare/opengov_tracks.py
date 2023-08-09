import onchain.opengov_track_ids as ogt
import pandas as pd
from reports.governance_etl import GovernanceReport
from governance_governanceReport.sources.opensquare import Extractor, Transformer
from string import Template
from concurrent.futures import ThreadPoolExecutor


def get_data():
    fields = ["id", "name", "origin", "description", "maxDeciding",
              "decisionDeposit", "preparePeriod", "decisionPeriod",
              "confirmPeriod", "minEnactmentPeriod",
              "minApproval_linearDecreasing_length",
              "minApproval_linearDecreasing_floor",
              "minApproval_linearDecreasing_ceil",
              "minApproval_reciprocal_factor", "minApproval_reciprocal_xOffset",
              "minApproval_reciprocal_yOffset",
              "minSupport_linearDecreasing_length",
              "minSupport_linearDecreasing_floor",
              "minSupport_linearDecreasing_ceil",
              "minSupport_reciprocal_factor", "minSupport_reciprocal_xOffset",
              "minSupport_reciprocal_yOffset"]
    token_cols = ["decisionDeposit"]
    route_template = Template("/gov2/tracks/$track_id")

    df = pd.DataFrame([])
    for network in GovernanceReport().networks:
        track_ids = ogt.get_data(network)
        routes = [route_template.substitute(track_id=tid) for tid in track_ids]
        with ThreadPoolExecutor() as exe:
            futures = [
                exe.submit(Extractor("subsquare", network, route).extract)
                for route in routes]
        data = [future.result() for future in futures]

        df_chain = Transformer(data).transform(fields, token_cols, network,
                                               sort_by=fields[0])
        df_chain["network"] = network
        df = pd.concat([df, df_chain], ignore_index=True)
    df = df.reindex(columns=["network"] + fields)

    return df


if __name__ == "__main__":
    tracks = get_data()
    print(tracks.to_string())
