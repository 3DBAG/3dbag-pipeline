QUERY_BACKFILL_STATUS = """
query BackfillPartitionStatus($backfillId: String!) {
  partitionBackfillOrError(backfillId: $backfillId) {
    __typename
    ... on PartitionBackfill {
      id
      status
      partitionStatuses {
        results {
          partitionName
          runId
          runStatus
        }
      }
      partitionStatuses {results {partitionName }}
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""

variables = {
    "backfillId": "backfill id",
}
reload_res = requests.post(
    "http://{dagit_host}:3000/graphql?query={query_string}&variables={variables}".format(
        dagit_host=dagit_host,
        query_string=RELOAD_REPOSITORY_LOCATION_MUTATION,
        variables=json.dumps(variables),
    )
).json()

"""
{
  "data": {
    "partitionBackfillOrError": {
      "__typename": "PartitionBackfill",
      "id": "byauudaj",
      "status": "COMPLETED",
      "partitionStatuses": {
        "results": [
          {
            "partitionName": "30dz2",
            "runId": "bd06fd2d-c11f-4cac-95a5-7da5a2da1bab",
            "runStatus": "STARTED"
          },
          {
            "partitionName": "30gz1",
            "runId": "d2e8d73a-9673-4101-a0e5-cdd356603a50",
            "runStatus": "STARTED"
          },
          {
            "partitionName": "37bn1",
            "runId": "5b545bfe-6c1b-4028-b467-118ef505b530",
            "runStatus": "SUCCESS"
          },
          {
            "partitionName": "37bn2",
            "runId": "b1c66cc6-c041-438c-815b-e051be4052a2",
            "runStatus": "SUCCESS"
          },
          {
            "partitionName": "37en1",
            "runId": "1f4618a8-be67-444b-98fe-a22d693a1fb2",
            "runStatus": "SUCCESS"
          }
        ]
      }
    }
  }
}
"""
