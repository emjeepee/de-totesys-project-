import pytest
import pandas as pd

from unittest.mock import Mock, patch, ANY
from src.third_lambda.third_lambda_utils.make_row_query_for_correct_table import make_row_query_for_correct_table




@pytest.fixture(scope="function")
def general_setup():
    # make_row_query_for_correct_table(table_name: str, pk_col: str, df_cols, vals_lst: list)
    tn_dim = 'design'

    tn_fct = 'sales_order'

    pk_col_dim = 'design_id'

    pk_col_fct = 'sales_order_id'

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
    
    # make values lists for dimension table
    # (only need to use one in the tests):
    dim_vls_lst_1 = [ '15', 'a15', 'b15', 'c15'  ]
    dim_vls_lst_2 = [ '16', 'a16', 'b16', 'NULL'  ]
    # dim_vls_lst = [{str(val)}    if val is not None else "NULL"    for val in row_data]

    # make values lists for fact table
    # (only need to use one in the tests):
    fact_vls_lst_1 = [ '212', 'a15', 'b15', 'c15'  ]
    fact_vls_lst_2 = [ '213', 'a16', 'b16', 'c16'  ]

    # dim columns:
    dim_cols_str   = '(design_id, dx, dy, dz)'  # cols_str is '(xxx, yyy, zzz)'

    # dim first row:
    dim_vals_str_1 = '(15, a15, b15, c15)'    # '(1, NULL, turnip)'
    dim_c_v_prs_1  = 'design_id=15, dx=a15, dy=b15, dz=c15'   # 'xxx = 1, yyy = NULL, zzz = turnip;'

    # dim 2nd row:
    dim_vals_str_2 = '(16, a16, b16, c16)'
    dim_c_v_prs_2  = 'design_id=16, dx=a16, dy=b16, dz=c16'

    # make mock dataFrames and get column names:
    df_dim = pd.DataFrame(mock_dim_tbl)
    df_dim_cols = df_dim.columns
    df_fact = pd.DataFrame(mock_fact_tbl)
    df_fact_cols = df_fact.columns

    # make_row_query_for_correct_table(table_name: str, pk_col: str, df_cols, vals_lst: list)
    yield tn_dim, tn_fct, pk_col_dim, pk_col_fct, df_dim_cols, df_fact_cols, dim_vls_lst_1, fact_vls_lst_1




# @pytest.mark.skip
def test_returns_correct_output_depending_on_passed_in_table_name(general_setup):
    # Arrange:
    (tn_dim, tn_fct, pk_col_dim, pk_col_fct, df_dim_cols, df_fact_cols, dim_vls_lst_1, fact_vls_lst_1) = general_setup
    
    
    # Act and assert:
    # patch the functions one of which 
    # make_row_query_for_correct_table() 
    # will call:
    with patch('src.third_lambda.third_lambda_utils.make_row_query_for_correct_table.make_query_for_one_row_dim_table') as mqfor_dim_tbl, \
         patch('src.third_lambda.third_lambda_utils.make_row_query_for_correct_table.make_query_for_one_row_fact_table') as mqfor_fact_tbl:
        
        # mock returns values of the two
        # functions one of which 
        # make_row_query_for_correct_table() 
        # will call:
        mqfor_dim_tbl.return_value = 'mock_mqfor_dim_tbl_return_value'
        mqfor_fact_tbl.return_value = 'mock_mqfor_fact_tbl_return_value'

        # Get return values of 
        # make_row_query_for_correct_table()
        # after passing in different table 
        # names:
        result_dim = make_row_query_for_correct_table('design', pk_col_dim, df_dim_cols, dim_vls_lst_1)
        # result_dim = None
        mqfor_dim_tbl.assert_called_once_with('design', pk_col_dim, df_dim_cols, dim_vls_lst_1)
        assert result_dim == 'mock_mqfor_dim_tbl_return_value'


        result_fct = make_row_query_for_correct_table('sales_order', pk_col_fct, df_fact_cols, fact_vls_lst_1)
        # result_fct = None
        mqfor_fact_tbl.assert_called_once_with('sales_order', df_fact_cols, fact_vls_lst_1)
        assert result_fct == 'mock_mqfor_fact_tbl_return_value'


