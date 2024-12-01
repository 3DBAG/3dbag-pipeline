"""Parse all dagster debug files and find the building IDs that are unfinished in the
reconstruction.

The dagster debug files are downloaded from the UI. Go to the Runs overview to see the
individual runs, the drop-down menu of the 'View' button of the run has a
'Download debug file' option, click that.

Download all .gz files into a directory. That dirpath is the --debug-dir input.

The script extracts the '[reconsturctor] start: <building path>' and
'[reconstructor] finish: <building path>' records, matches the start-finish IDs and
prints the IDs that don't have a 'finish' record.
"""

import argparse
import gzip
import json
import re
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument(
    "--debug-dir",
    help="Directory with the dagster debug files that are gzipped.",
    type=Path,
)

if __name__ == "__main__":
    args = parser.parse_args()

    # (ID, started, finished)
    buildings_started = []
    buildings_finished = []
    re_pat_start = re.compile(r"(?<=\[reconstructor\] start:) (/.*?\.\S*)")
    re_pat_finish = re.compile(r"(?<=\[reconstructor\] finish:) (/.*?\.\S*)")
    for p_gz in args.debug_dir.iterdir():
        with gzip.open(p_gz, "rb") as f:
            debug_messages = json.load(f)
            for event in debug_messages["event_list"]:
                if event["step_key"] == "reconstructed_building_models_nl":
                    record = event["user_message"]
                    if (res := re_pat_start.search(record)) is not None:
                        building_id = Path(res.group(1)).stem.rstrip(".city")
                        buildings_started.append(building_id)
                    elif (res := re_pat_finish.search(record)) is not None:
                        building_id = Path(res.group(1)).stem.rstrip(".city")
                        buildings_finished.append(building_id)
                    else:
                        continue
    buildings_unfinished = set(buildings_started) - set(buildings_finished)
    print(buildings_unfinished)
