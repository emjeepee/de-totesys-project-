import pytest
from unittest.mock import patch, Mock
from third_lambda.second_util_for_3rd_lambda import (
    put_table_data_in_warehouse,
    convert_dataframe_to_SQL_query_string,
)


# @pytest.mark.skip(reason="Skipping this test to perform only the previous tests")

