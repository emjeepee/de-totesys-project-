
from datetime import datetime


def make_timestamp():
    timestamp = datetime.now()
    # print(f'This is the timestamp string >>> {timestamp}')
    return timestamp


fixed_time = datetime.datetime(2023, 1, 1, 12, 0, 0)

# TS = make_timestamp()
# print(f'This is the datetime object >>> {TS} and its type is {type(TS)}')



def create_timestamp(timestamp):
    """
    This function:
        1) creates a timestamp string of the format
            'YYYY-MM-DD_HH-MM-SS'
    Returns:
        The formatted timestamp string
    Args:
        A timestamp string created by datetime.now(),
        for example: '2025-05-29 22:17:19.251352'
    """
    formatted_ts = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_ts

formatted_ts = create_timestamp(fixed_time)

print(f'This is the formatted timestamp >>> {formatted_ts}')





@pytest.fixture(scope="module")
def mock_S3_client():
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")

def test_get_most_recent_table_data_returns_correct_list():
        # Create mock data
    timestamp = datetime.now()
    bucket_name = "11-ingestion-bucket"
    object_key = f"design/{timestamp}.json"


    # Create mock bucket
    mock_S3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                                )        