"""
assert_dynamo.py
----------------
Used by the integration test CI workflow to verify that at least one
KPI item was written into DynamoDB after a pipeline execution.
Exits 0 if items exist, exits 1 if the table is empty or unreachable.
"""

import argparse
import boto3
import sys


def assert_items_exist(table_name: str, region: str):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table    = dynamodb.Table(table_name)

    # Use a lightweight scan with a limit of 1 – we only need to confirm
    # at least one item exists, not retrieve them all.
    resp  = table.scan(Limit=1)
    count = resp.get("Count", 0)

    if count > 0:
        item = resp["Items"][0]
        print(f"PASSED – table '{table_name}' has items.")
        print(f"  Sample item keys: {list(item.keys())}")
        return 0
    else:
        print(f"FAILED – table '{table_name}' has no items. Pipeline may not have written data.")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table",  required=True, help="DynamoDB table name")
    parser.add_argument("--region", required=True, help="AWS region")
    args = parser.parse_args()
    sys.exit(assert_items_exist(args.table, args.region))
