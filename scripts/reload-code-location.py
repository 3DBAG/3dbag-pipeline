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

dagit_host = "your_dagit_host_here"

variables = {
    "repositoryLocationName": "your_location_name_here",
}
reload_res = requests.post(
    "http://{dagit_host}:3000/graphql?query={query_string}&variables={variables}".format(
        dagit_host=dagit_host,
        query_string=RELOAD_REPOSITORY_LOCATION_MUTATION,
        variables=json.dumps(variables),
    )
).json()

did_succeed = reload_res["data"]["reloadRepositoryLocation"]["__typename"] == "RepositoryLocation"