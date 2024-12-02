import argparse
import json

import requests

parser = argparse.ArgumentParser()
parser.add_argument("--host", help="dagster host", default="localhost")
parser.add_argument("--port", help="dagster port", type=int, default=3000)
parser.add_argument("--code-location", help="dagster code location", default="core")

RELOAD_REPOSITORY_LOCATION_MUTATION = """
mutation ($repositoryLocationName: String!) {
   reloadRepositoryLocation(repositoryLocationName: $repositoryLocationName) {
      __typename
      ... on RepositoryLocation {
        name
        repositories {
            name
        }
        isReloadSupported
      }
      ... on RepositoryLocationLoadFailure {
          name
          error {
              message
          }
      }
   }
}
"""

if __name__ == "__main__":
    args = parser.parse_args()

    variables = {
        "repositoryLocationName": args.code_location,
    }
    reload_res = requests.post(
        "http://{dagster_host}:{dagster_port}/graphql?query={query_string}&variables={variables}".format(
            dagster_host=args.host,
            dagster_port=args.port,
            query_string=RELOAD_REPOSITORY_LOCATION_MUTATION,
            variables=json.dumps(variables),
        )
    ).json()

    did_succeed = (
        reload_res["data"]["reloadRepositoryLocation"]["__typename"]
        == "RepositoryLocation"
    )

    exit(not did_succeed)
