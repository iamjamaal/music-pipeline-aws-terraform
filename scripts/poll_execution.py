"""
poll_execution.py
-----------------
Used by the integration test CI workflow to wait for a Step Functions
execution to reach a terminal state.  Exits 0 on SUCCEEDED, exits 1
on any failure state or timeout.
"""

import argparse
import boto3
import sys
import time
from datetime import datetime


def poll(execution_arn: str, timeout_minutes: int, poll_interval: int):
    client   = boto3.client("stepfunctions")
    deadline = time.time() + timeout_minutes * 60
    print(f"Polling execution: {execution_arn}")
    print(f"Timeout: {timeout_minutes} minutes  |  Interval: {poll_interval}s")

    while time.time() < deadline:
        resp   = client.describe_execution(executionArn=execution_arn)
        status = resp["status"]
        print(f"[{datetime.utcnow().isoformat()}] Status: {status}")

        if status == "SUCCEEDED":
            print("PASSED – execution completed successfully.")
            return 0
        if status in ("FAILED", "TIMED_OUT", "ABORTED"):
            cause = resp.get("cause", "No cause provided.")
            error = resp.get("error", "Unknown")
            print(f"FAILED – execution ended with status: {status}")
            print(f"  Error : {error}")
            print(f"  Cause : {cause}")
            return 1

        time.sleep(poll_interval)

    print(f"TIMEOUT – execution did not complete within {timeout_minutes} minutes.")
    return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-arn",         required=True)
    parser.add_argument("--timeout-minutes",        type=int, default=15)
    parser.add_argument("--poll-interval-seconds",  type=int, default=30)
    args = parser.parse_args()
    sys.exit(poll(args.execution_arn, args.timeout_minutes, args.poll_interval_seconds))
