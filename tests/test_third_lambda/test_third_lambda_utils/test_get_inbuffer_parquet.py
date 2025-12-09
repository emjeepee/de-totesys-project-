import os
import boto3
import pytest
import logging

from io import BytesIO
from unittest.mock import Mock, patch, ANY
from moto import mock_aws
from botocore.exceptions import ClientError

from src.third_lambda.third_lambda_utils.get_inbuffer_parquet import get_inbuffer_parquet
from src.third_lambda.third_lambda_utils.errors_lookup        import errors_lookup





def test_returns_buffer():
    # arrange
    object_key = "file.parquet"
    table_name = "mock_table_name"
    bucket = 'mock_bucket'

    mock_bytes = b"mock_parquet_file"
    mock_body = Mock()
    mock_body.read.return_value = mock_bytes

    mock_s3 = Mock()
    mock_s3.get_object.return_value = {
        "Body": mock_body
                                      }

    # act:
    result = get_inbuffer_parquet(mock_s3, 
                          object_key, 
                          bucket,
                          table_name)

    # assert:
    # Assert
    assert isinstance(result, BytesIO)
    result.seek(0)
    assert result.read() == mock_bytes

    mock_s3.get_object.assert_called_once_with(
        Key=object_key,
        Bucket=bucket,
                                              )    
    


def test_raises_client_error():
    # Arrange
    mock_s3_client = Mock()

    mock_s3_client.get_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "500", "Message": "S3 failed"}
            },
            operation_name="GetObject"
                                                            )

    # act and assert
    with pytest.raises(ClientError):
            get_inbuffer_parquet(
                s3_client=mock_s3_client,
                object_key="some/key.parquet",
                bucket="processed-bucket",
                table_name="sales"
                                )
            



def test_logs_error_correctly(caplog):
    # Arrange
    mock_s3_client = Mock()

    mock_s3_client.get_object.side_effect = ClientError(
        error_response={
            "Error": {"Code": "500", "Message": "Boom"}
        },
        operation_name="GetObject"
    )

    caplog.set_level(logging.ERROR)

    # Act
    with pytest.raises(ClientError):
        get_inbuffer_parquet(
            s3_client=mock_s3_client,
            object_key="key",
            bucket="bucket",
            table_name="mock_table_name"
        )

    # Assert
    assert any(
        errors_lookup["err_0"] + 'mock_table_name' in message
        for message in caplog.messages
    )            