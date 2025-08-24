import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from unittest.mock import Mock, patch, ANY, call
from io import BytesIO

from src.third_lambda.third_lambda_utils.make_SQL_queries import make_SQL_queries



@pytest.fixture(scope="function")
def general_setup():

    tn_so = 'sales_order'
    tn_di = 'design'

    # make a mock dimension table
    # and a mock of the fact table: 
    mock_dim_tbl  = [
        {'design_id': 15, 'dx': 'a15', 'dy': 'b15', 'dz': 'c15'  },
        {'design_id': 16, 'dx': 'a16', 'dy': 'b16', 'dz': None  }
                    ]
    
    mock_fact_tbl  = [
        {'sales_order_id': 212, 'fx': 'a15', 'fy': 'b15', 'fz': 'c15'  },
        {'sales_order_id': 213, 'fx': 'a16', 'fy': 'b16', 'fz': 'c16'  }
                    ]

    # name of primary key:
    pk_dim_tbl = 'design_id'

    dim_vls_lst_1 = [ '15', 'a15', 'b15', 'c15'  ]
    dim_vls_lst_2 = [ '16', 'a16', 'b16', 'NULL'  ]
    # dim_vls_lst = [{str(val)}    if val is not None else "NULL"    for val in row_data]

    # dim columns:
    dim_cols_str   = '(design_id, dx, dy, dz)'  # cols_str is '(xxx, yyy, zzz)'

    # dim first row:
    dim_vals_str_1 = '(15, a15, b15, c15)'    # '(1, NULL, turnip)'
    dim_c_v_prs_1  = 'design_id=15, dx=a15, dy=b15, dz=c15'   # 'xxx = 1, yyy = NULL, zzz = turnip;'

    # dim 2nd row:
    dim_vals_str_2 = '(16, a16, b16, c16)'
    dim_c_v_prs_2  = 'design_id=16, dx=a16, dy=b16, dz=c16'


# [cols_str, vals_str, col_val_pairs]
    exp_dim_rw_1_qy = f"INSERT INTO design {dim_cols_str} VALUES {dim_vals_str_1} ON CONFLICT design_id DO UPDATE SET {dim_c_v_prs_1}"
    exp_dim_rw_2_qy = f"INSERT INTO design {dim_cols_str} VALUES {dim_vals_str_2} ON CONFLICT design_id DO UPDATE SET {dim_c_v_prs_2}"

    exp_dim_qr_lst = [exp_dim_rw_1_qy, exp_dim_rw_2_qy]            

    # make mock dataFrames:
    df_dim = pd.DataFrame(mock_dim_tbl)
    df_fact = pd.DataFrame(mock_fact_tbl)

    yield tn_so, tn_di, pk_dim_tbl, dim_vls_lst_1, dim_vls_lst_2, df_dim, df_fact, exp_dim_qr_lst






# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange:
    (tn_so, tn_di, pk_dim_tbl, dim_vls_lst_1, dim_vls_lst_2, df_dim, df_fact, exp_dim_qr_lst) = general_setup
    expected = list

    # Act:
    # make_SQL_queries(df, table_name: str)
    response_dim = make_SQL_queries(df_dim, tn_so)
    response_fct = make_SQL_queries(df_fact, tn_di)
    result_dim = type(response_dim)
    result_fct = type(response_fct)
    # result_dim = None
    # result_fct = None

    # Assert:
    assert result_dim == expected
    assert result_fct == expected




# @pytest.mark.skip
def test_correctly_calls_function_make_row_query_for_correct_table(general_setup):
    # Arrange:
    (tn_so, tn_di, pk_dim_tbl, dim_vls_lst_1, dim_vls_lst_2, df_dim, df_fact, exp_dim_qr_lst) = general_setup

    expected = ["output_1", "output_1"]

    # Act and assert:
    with patch('src.third_lambda.third_lambda_utils.make_SQL_queries.make_row_query_for_correct_table') as mock_mrqfct:
        mock_mrqfct.side_effect = ["output_1", "output_1"]

        # make_SQL_queries(df, table_name: str)
        result = make_SQL_queries(df_dim, tn_di)

        # make_row_query_for_correct_table(table_name, pk_col, df.columns, vals_lst)
        expected_calls = [
            call(tn_di, pk_dim_tbl, df_dim.columns, dim_vls_lst_1),
            call(tn_di, pk_dim_tbl, df_dim.columns, dim_vls_lst_2)
                        ]
        
        assert mock_mrqfct.call_count == 2
        mock_mrqfct.assert_has_calls(expected_calls)
        assert result == expected











@pytest.mark.skip
def test_returns_correct_list_for_dim_table(general_setup):
    # Arrange:
    (tn_so, tn_di, df_dim, df_fact, exp_dim_qr_lst) = general_setup
    
    expected_dim = exp_dim_qr_lst

    # Act:
    # make_SQL_queries(df, table_name: str)
    result_dim = make_SQL_queries(df_dim, tn_di)
    # result_dim = None

    # Assert:
    assert result_dim == expected_dim

