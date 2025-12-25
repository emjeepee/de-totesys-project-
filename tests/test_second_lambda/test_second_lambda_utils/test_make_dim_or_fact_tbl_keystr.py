

from src.second_lambda.second_lambda_utils.make_dim_or_fact_tbl_keystr import make_dim_or_fact_tbl_keystr






def test_returns_a_string():
    # arrange:
    expected = str

    # act:
    response_0 = make_dim_or_fact_tbl_keystr('sales_order', 'timestamp')
    response_1 = make_dim_or_fact_tbl_keystr('design', 'timestamp')
    result_0 = type(response_0)
    result_1 = type(response_1)

    # assert:
    result_0 = expected
    result_1 = expected



def test_returns_correct_string():
    # arrange:
    expected_0 = "fact_sales_order/timestamp.parquet"
    expected_1 = "dim_design/timestamp.parquet"

    # act:
    result_0 = make_dim_or_fact_tbl_keystr('sales_order', 'timestamp')
    result_1 = make_dim_or_fact_tbl_keystr('design', 'timestamp')

    # assert:
    result_0 = expected_0
    result_1 = expected_1


