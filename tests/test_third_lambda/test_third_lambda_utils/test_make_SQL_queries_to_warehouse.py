import pytest

from unittest.mock import Mock, patch, ANY
from src.third_lambda.third_lambda_utils.make_SQL_queries_to_warehouse import make_SQL_queries_to_warehouse

# make_SQL_queries_to_warehouse(qrs_list: list, conn)

def test_raises_RuntimeError():
    conn = Mock()
    conn.run.side_effect = Exception("Failure to contact data warehouse")
    with pytest.raises(RuntimeError):
            make_SQL_queries_to_warehouse(ANY, conn)
            return
