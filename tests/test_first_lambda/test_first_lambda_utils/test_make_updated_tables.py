import pytest


from unittest.mock import Mock, patch, ANY


from src.first_lambda.first_lambda_utils.make_updated_tables import make_updated_tables


@pytest.fixture(scope="module")
def set_up():

    mock_old_de_table = [
            {"design_id": 1, "name": "abdul", "team": 1, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 2, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"},
                             ]

    mock_updated_de_rows = [
            {"design_id": 1, "name": "abdul", "team": 42, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 13, "project": "terraform"}
                            ]

    expected_updated_de_table = [
            {"design_id": 1, "name": "abdul", "team": 42, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 13, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"},
                             ]


    mock_old_so_table = [
            {"sales_order_id": 1, "name": "aaa", "product": "iPhone17"},
            {"sales_order_id": 2, "name": "Mukund", "product": "iPad"},
            {"sales_order_id": 3, "name": "bbb", "product": "iMac"},
                             ]

    mock_updated_so_rows = [
            {"sales_order_id": 1, "name": "aaa", "product": "artechoke"},
            {"sales_order_id": 2, "name": "Mukund", "product": "iMac"}
                            ]
        
    expected_updated_so_table = [
            {"sales_order_id": 1, "name": "aaa", "product": "artechoke"},
            {"sales_order_id": 2, "name": "Mukund", "product": "iMac"},
            {"sales_order_id": 3, "name": "bbb", "product": "iMac"},
                             ]

    # must contain updated 
    # rows (only) for each 
    # table:
    mock_data_for_s3 =             [
            {'design': mock_updated_de_rows},  
            {'sales_order': mock_updated_so_rows} 
                                   ]



    yield mock_old_de_table, mock_updated_de_rows, expected_updated_de_table, mock_old_so_table, mock_updated_so_rows, expected_updated_so_table, mock_data_for_s3






# test:
# calls get_most_recent_table_data() correct number of times and with right args
# calls update_rows_in_table() correct number of times and with right args
# returns correct value


def test_calls_functions_correctly(set_up):
    # arrange:
    mock_old_de_table, mock_updated_de_rows, expected_updated_de_table, mock_old_so_table, mock_updated_so_rows, expected_updated_so_table, mock_data_for_s3 = set_up
    expected = list
    expected_0 = dict

    mock_s3_client = Mock 
    mock_bucket = Mock   


    with patch ('src.first_lambda.first_lambda_utils.make_updated_tables.get_most_recent_table_data') as mock_gmrtd, \
        patch ('src.first_lambda.first_lambda_utils.make_updated_tables.update_rows_in_table') as mock_urit:
        mock_gmrtd.side_effect = [mock_old_de_table, mock_old_so_table]
        mock_urit.side_effect = [expected_updated_de_table, expected_updated_so_table]

        # act:
        response = make_updated_tables(mock_data_for_s3,
                        mock_s3_client,
                        mock_bucket)
        
        assert mock_gmrtd.call_count == 2
        assert mock_urit.call_count == 2
    
        mock_gmrtd.assert_any_call('design', mock_s3_client, mock_bucket)
        mock_gmrtd.assert_any_call('sales_order', mock_s3_client, mock_bucket)

        mock_urit.assert_any_call(mock_updated_de_rows, mock_old_de_table, 'design')
        mock_urit.assert_any_call(mock_updated_so_rows, mock_old_so_table, 'sales_order')
