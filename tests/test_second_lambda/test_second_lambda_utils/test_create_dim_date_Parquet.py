import pytest

from datetime import datetime
from src.second_lambda.second_lambda_utils.create_dim_date_Parquet import create_dim_date_Parquet


# from second_lambda.second_lambda_utils.make_dim_date_python import make_dim_date_python
# from src.second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquet



# @pytest.mark.skip
def test_returns_a_list():
    # Arrange:
    expected = list
    start_date = datetime(24, 1, 1)


    # Act:
    # create_dim_date_Parquet(start_date, timestamp_string: str, num_rows: int)
    result = None
    reponse = create_dim_date_Parquet(start_date, "2025-08-14_12-33-27", 3)
    # result = type(reponse)



    # Assert:
    assert result == expected 
    pass




@pytest.mark.skip
def test_xx():
    # Arrange:

    # Act:

    # Assert:
    pass