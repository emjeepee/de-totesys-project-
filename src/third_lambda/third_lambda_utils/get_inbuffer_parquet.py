import logging

from io import BytesIO
from botocore.exceptions import ClientError

from . return_inbuffer_parquet_file import return_inbuffer_parquet_file
from .errors_lookup import errors_lookup


logger = logging.getLogger(__name__)


def get_inbuffer_parquet(
                         s3_client,
                         object_key: str,
                         bucket: str,
                         table_name: str
                         ):
    """
    This function:
        looks in the processed
        bucket and from it gets
        a buffer that contains
        a Parquet file.

    Args:
        s3_client: a boto3 S3
        client.

        object_key: the key
        under which the
        processed bucket stores
        the buffer that
        contains the Parquet
        file.

        bucket: the name of
        the bucket.

        table_name: the name
        of the table.

    returns:
        a buffer that contains 
        the parquet file.

    """

    try:
        pq_buff = return_inbuffer_parquet_file(s3_client,
                                               object_key,
                                               bucket
                                              )
        return pq_buff

    except ClientError:
        # log exception
        # and stop code:
        logger.error(errors_lookup["err_0"] + table_name)
        raise



