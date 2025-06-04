from src.conn_to_db import conn_to_db
from unittest.mock import Mock, patch, ANY
import pytest
import os
from src.conn_to_db import conn_to_db


def test_conn_to_db_connects_to_the_database():
    with patch("src.conn_to_db.Connection") as mock_Connection_to_db:
        DB_NAME = 'TESTDB'
        os.environ[f"PG_{DB_NAME}_USERNAME"] = 'test_db_name'
        os.environ[f"PG_{DB_NAME}_PASSWORD"] = 'test_db_name'
        os.environ[f"PG_{DB_NAME}_DATABASE"] = 'test_db_name'
        os.environ[f"PG_{DB_NAME}_HOST"] = 'test_db_name'
        os.environ[f"PG_{DB_NAME}_PORT"] = '12345'

        return_value = conn_to_db()
        assert return_value == mock_Connection_to_db(ANY, ANY, ANY, ANY, ANY, ANY)


# def test_thatconn_to_db 
