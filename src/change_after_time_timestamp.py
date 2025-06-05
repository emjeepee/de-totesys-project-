from src.utils_write_to_ingestion_bucket import create_formatted_timestamp

import botocore
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, UTC


def change_after_time_timestamp(bucket_name, s3_client, ts_key, default_ts):
    """
    This function:
        1) is the first utility function that the
                first lambda function calls.
        2) will in a try-except block try to read
                the last timestamp that has been
                saved in the ingestion bucket. The
                very first value (ie the string that
                represents the year 1900) should have
                been saved in the ingestion bucket
                during the setting up of the project.
                Under 'try:' this function should set
                variable last_TS to that saved value.
                Under 'except:' this function should
                set last_TS to the timestamp that
                represents the year 1900.
                Under 'try:' and 'except:' this
                function should return the read
                timestamp so that the next utility
                function can use it.
        3) calculates the new time stamp and puts it
                in the ingestion bucket under the
                same key as the current timestamp
                (which replaces the previous time stamp).
                The value of the time represented
                by the new timestamp must
                represent the time when this
                function runs.

    Args:
        bucket_name: a string for the name of the
            ingestion bucket.
        s3_client: the boto3 S3 client.
        ts_key: the key for the object in the
            ingestion bucket that is the current
            timestamp string.
        default_ts: a timestamp string that the
            function will return on its very first
            read of the ingestion bucket (eg
            "1900-01-01-00-00-00")


    Returns:
        Either --
            a) the previously saved timestamp
            string if the operation to read the appropriate
            object in the ingestion bucket was
            successful. The first time ever that
            the lambda function runs this will be
            the timestamp for the year 1900
            b) the timestamp string representing the year
            1900 if the read operation
            to read the appropriate
            object in the ingestion bucket failed.

    """

    # create now+5minutes timestamp string.
    # First create a now timestamp string:
    # now_ts = create_formatted_timestamp()
    now_ts_with_ms = datetime.now(UTC).isoformat()
    now_ts = now_ts_with_ms[:-13]
    # now_ts is a string like this: "2025-06-04T08:28:12"

    # now_dt = datetime.strptime(now_ts, "%Y-%m-%d_%H-%M-%S")
    # now_ts = now_dt.strftime("%Y-%m-%d_%H-%M-%S")

    # ts_key will always be the same key, eg "***timestamp***"

    try:
        # get previous timestamp from bucket:
        response = s3_client.get_object(Bucket=bucket_name, Key=ts_key)
        # replace previous timestamp in bucket with new timestamp:
        s3_client.put_object(Bucket=bucket_name, Key=ts_key, Body=now_ts)
        return response["Body"].read().decode("utf-8")
    except ClientError:
        return default_ts
