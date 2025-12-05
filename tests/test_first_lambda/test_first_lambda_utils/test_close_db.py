import os
import pytest
import logging

from pg8000.native import Error, Connection
from unittest.mock import Mock, patch, ANY

from src.first_lambda.first_lambda_utils.conn_to_db import close_db
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup




@pytest.fixture
def setup_test_env():
    """
    Set up test environment 
    variables.
    """
    os.environ["TESTING_DB_USER"] = "test_user"
    os.environ["TESTING_DB_PASSWORD"] = "test_password"
    os.environ["TESTING_DB_DB"] = "test_database"
    os.environ["TESTING_DB_HOST"] = "test_host"
    os.environ["TESTING_DB_PORT"] = "test_port"

    test_user = "test_user"
    test_password = "test_password"
    test_database = "test_database"
    test_host = "test_host"
    test_port = "test_port"


    yield test_user, test_password, test_database, test_host, test_port

    # clean up:
    for key in ["TESTING_DB_USER", 
                "TESTING_DB_PASSWORD", 
                "TESTING_DB_DB", 
                "TESTING_DB_HOST", 
                "TESTING_DB_PORT"]:
        # Delete env var called 
        # key; don't do anything
        # if it doesn't exist:
        os.environ.pop(key, None)




def test_close_db_calls_method_close_(setup_test_env):
    # arrange:

    with patch('src.first_lambda.first_lambda_utils.conn_to_db.Connection') as mock_Conn:
        close = Mock()
        close.return_value = None
        mock_Conn.close = close
        # act:
        close_db(mock_Conn)

    # assert  
    close.assert_called_once()


def test_close_db_logs_exception_correctly(caplog):
    # arrange:

    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="conn_to_db")

    with patch('src.first_lambda.first_lambda_utils.conn_to_db.Connection') as mock_Conn:
        # Make Connection() raise 
        # Error immediately:
        close = Mock()
        close.side_effect = Error(
            {"Error": {
                "Code": "500",
                "Message": "Failed to connect to database"
            }},
            "Connection"
                                )
        mock_Conn.close = close

        # because the function 
        # re-raises the error:
        with pytest.raises(Error): 
            close_db(mock_Conn)        


    # check that error 
    # was logged:
    assert any(errors_lookup['err_9'] in msg for msg in caplog.messages)


