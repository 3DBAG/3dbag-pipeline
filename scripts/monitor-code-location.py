"""Check if a code location has loaded without errors."""

import argparse
import json

import requests

parser = argparse.ArgumentParser()
parser.add_argument("--host", help="dagster host", default="localhost")
parser.add_argument("--port", help="dagster port", type=int, default=3000)
parser.add_argument("--code-location", help="dagster code location", default="core")

QUERY_CODE_LOCATIONS = """
query LocationStatusesQuery($codeLocationName: String!) {
  workspaceLocationEntryOrError(name: $codeLocationName) {
    ... on WorkspaceLocationEntry {
      id
      name
      loadStatus
      locationOrLoadError
    }
  }
}
"""

if __name__ == "__main__":
    args = parser.parse_args()

    variables = {
        "codeLocationName": args.code_location,
    }
    reload_res = requests.post(
        "http://{dagster_host}:{dagster_port}/graphql?query={query_string}&variables={variables}".format(
            dagster_host=args.host,
            dagster_port=args.port,
            query_string=QUERY_CODE_LOCATIONS,
            variables=json.dumps(variables),
        )
    ).json()

    location_entry = reload_res["data"]["workspaceLocationEntry"]
    load_status = location_entry.get("loadStatus")
    location_or_error = location_entry.get("locationOrError").get("__typename")
    is_loaded = load_status == "LOADED" and location_or_error == "RepositoryLocation"

    exit(not is_loaded)
