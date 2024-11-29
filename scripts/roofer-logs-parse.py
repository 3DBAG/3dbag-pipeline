import argparse
import csv
import re
from pathlib import Path

import requests

parser = argparse.ArgumentParser()
parser.add_argument("--host", help="dagster host", default="localhost")
parser.add_argument("--port", help="dagster port", type=int, default=3000)
parser.add_argument(
    "--storage",
    help="dagster storage location",
    required=True,
    type=Path,
    default=Path("/opt/dagster/dagster_home/storage"),
)
parser.add_argument("-o", "--output", required=True, type=Path)

ROOFER_SUCCESS_QUERY = """
query FilteredRunsQuery {
  runsOrError(
    filter: { statuses: [SUCCESS] pipelineName: "nl_reconstruct_debug" }
  ) {
    __typename
    ... on Runs {
      results {
        runId
        jobName
        status
        runConfigYaml
        startTime
        endTime
        pipelineName
      }
    }
  }
}
"""

if __name__ == "__main__":
    args = parser.parse_args()
    dagster_host = args.host
    dagster_port = args.port
    dagster_storage = args.storage

    roofer_res = requests.post(
        "http://{dagster_host}:{dagster_port}/graphql?query={query_string}".format(
            dagster_host=dagster_host,
            dagster_port=dagster_port,
            query_string=ROOFER_SUCCESS_QUERY,
        )
    ).json()

    logpaths_stderr = [
        p
        for i in roofer_res["data"]["runsOrError"]["results"]
        for p in dagster_storage.joinpath(i["runId"], "compute_logs").glob("*.err")
        if p.is_file()
    ]

    # "/opt/dagster/dagster_home/storage/0dbb1fa5-6dbf-454a-aed0-d94ec8d34fc0/compute_logs/ecsdmnmy.err"
    #
    # record = "2024-11-29 10:26:25 +0000 - dagster - INFO - nl_reconstruct_debug - 0dbb1fa5-6dbf-454a-aed0-d94ec8d34fc0 - reconstructed_building_models_nl - [2024-11-29 10:26:21.441] [stdout] [debug] [reconstructor t] /data/3DBAG/crop_reconstruct/6/240/8/objects/NL.IMBAG.Pand.1709100000243562/reconstruct/NL.IMBAG.Pand.1709100000243562.city.jsonl ((ArrangementOptimiser, 0),(ArrangementBuilder, 0),(SegmentRasteriser, 6),(LineRegulariser, 0),(PlaneIntersector, 0),(extrude, 2),(LineDetector, 0),(AlphaShaper_ground, 1),(PlaneDetector_ground, 2),(AlphaShaper, 0),(PlaneDetector, 1),)"
    # record = "2024-11-29 10:26:25 +0000 - dagster - INFO - nl_reconstruct_debug - 0dbb1fa5-6dbf-454a-aed0-d94ec8d34fc0 - reconstructed_building_models_nl - [2024-11-29 10:26:21.429] [stdout] [debug] [reconstructor] finish: /data/3DBAG/crop_reconstruct/6/240/8/objects/NL.IMBAG.Pand.1709100000244547/reconstruct/NL.IMBAG.Pand.1709100000244547.city.jsonl"

    records = []
    re_pat = re.compile(r"(?<=\[reconstructor t\]) (/.*?\.\S*) (.*)")
    with args.output.open("w") as f:
        csvwriter = csv.DictWriter(
            f,
            quoting=csv.QUOTE_STRINGS,
            fieldnames=[
                "building_id",
                "ArrangementOptimiser",
                "ArrangementBuilder",
                "SegmentRasteriser",
                "LineRegulariser",
                "PlaneIntersector",
                "extrude",
                "LineDetector",
                "AlphaShaper_ground",
                "PlaneDetector_ground",
                "AlphaShaper",
                "PlaneDetector",
            ],
        )
        csvwriter.writeheader()
        for logpath in logpaths_stderr:
            with logpath.open() as f:
                for record in f:
                    if (res := re.search(re_pat, record)) is not None:
                        record_parsed = {}
                        try:
                            if len(res.groups()) == 2:
                                # res.group(1) is eg.: /data/3DBAG/crop_reconstruct/6/240/8/objects/NL.IMBAG.Pand.1709100000243562/reconstruct/NL.IMBAG.Pand.1709100000243562.city.jsonl
                                building_id = Path(res.group(1)).stem.rstrip(".city")
                                record_parsed["building_id"] = building_id
                                # res.group(2) is eg: (ArrangementOptimiser, 0),(ArrangementBuilder, 0),(SegmentRasteriser, 6),(LineRegulariser, 0),(PlaneIntersector, 0),(extrude, 2),(LineDetector, 0),(AlphaShaper_ground, 1),(PlaneDetector_ground, 2),(AlphaShaper, 0),(PlaneDetector, 1),)
                                for i in res.group(2).split("),("):
                                    _k = i.strip(" ()").strip("(),").split(", ")
                                    record_parsed[_k[0]] = int(_k[1])
                                csvwriter.writerow(
                                    record_parsed
                                )  # records.append(record_parsed)
                        except Exception as e:
                            continue
