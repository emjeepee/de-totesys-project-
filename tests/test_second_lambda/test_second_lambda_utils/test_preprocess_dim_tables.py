import pytest

from src.second_lambda.second_lambda_utils.preprocess_dim_tables import preprocess_dim_tables

@pytest.fixture
def general_setup():
    keys_to_cut = ['aaa', 'bbb', 'ccc']
    table = [
            {'aaa': 1, 'bbb': 2, 'ccc': 3, 'ddd': 4, 'eee': 5},
            {'aaa': 1, 'bbb': 2, 'ccc': 3, 'ddd': 4, 'eee': 5},
            {'aaa': 1, 'bbb': 2, 'ccc': 3, 'ddd': 4, 'eee': 5}
                ]

    changed_table = [
            {'ddd': 4, 'eee': 5},
            {'ddd': 4, 'eee': 5},
            {'ddd': 4, 'eee': 5}
                ]

    yield keys_to_cut, table, changed_table





def test_preprocess_dim_tables_returns_list(general_setup):
    # Arrange
    (keys_to_cut, table, changed_table) = general_setup
    expected = list

    # Act
    result = type(preprocess_dim_tables(table, keys_to_cut))

    # Assert
    assert result == expected

    


# @pytest.mark.skip
def test_returns_list_of_same_length_as_argument_list(general_setup):
    # Arrange
    (keys_to_cut, table, changed_table) = general_setup
    expected = 3

    # Act
    result = len(preprocess_dim_tables(table, keys_to_cut))

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_returns_list_containing_only_dictionaries(general_setup):
    # Arrange
    (keys_to_cut, table, changed_table) = general_setup
    expected = True

    # Act
    list_1 = preprocess_dim_tables(table, keys_to_cut)
    result = all(isinstance(item, dict) for item in list_1)

    # Assert
    assert result == expected



    
# @pytest.mark.skip
def test_returns_correct_list(general_setup):
    # Arrange
    (keys_to_cut, table, changed_table) = general_setup
    expected = changed_table

    # Act
    result = preprocess_dim_tables(table, keys_to_cut)

    # Assert
    assert result == expected

