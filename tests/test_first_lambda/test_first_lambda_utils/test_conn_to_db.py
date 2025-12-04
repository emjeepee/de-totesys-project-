from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db
from unittest.mock import Mock, patch, ANY
import os



def test_conn_to_db_connects_to_the_database():
    with patch("src.first_lambda.first_lambda_utils.conn_to_db.Connection") as mock_Connection_to_db:
        DB_NAME = "TESTDB"
        os.environ[f"TF_{DB_NAME}_DB_USER"] = "test_db_name"
        os.environ[f"TF_{DB_NAME}_DB_PASSWORD"] = "test_db_pword"
        os.environ[f"TF_{DB_NAME}_DB_DB"] = "test_db_db_name"
        os.environ[f"TF_{DB_NAME}_DB_HOST"] = "test_db_host"
        os.environ[f"TF_{DB_NAME}_DB_PORT"] = "test_db_port"

        return_value = conn_to_db()
        
        assert return_value == mock_Connection_to_db(ANY, ANY, ANY, ANY, ANY, ANY)



