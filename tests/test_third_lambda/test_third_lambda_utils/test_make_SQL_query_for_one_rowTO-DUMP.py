import pytest







@pytest.fixture(scope="function")
def general_setup():
    # make_SQL_query_for_one_row(table_name: str, pk_col: str, cols: str, vals: str, row_data)
    t1 = 'design'

    t2 = 'sales_order'

    pk_col_des = 'design_id'

    pk_col_so = 'sales_record_id'

    cols_des = 'design_id, aaa, bbb, ccc'

    cols_so = 'sales_record_id, ddd, eee, fff'

    vals_des = '13, AAA, BBB, CCC'

    vals_so = '17, DDD, EEE, FFF'


    pass




@pytest.mark.skip
def test_xxxx(general_setup):
    # Arrange:
    
    # Act:
    # result = None

    #Assert:
    assert True





@pytest.mark.skip
def test_xxxx(general_setup):
    # Arrange:
    
    # Act:
    # result = None

    #Assert:
    assert True    