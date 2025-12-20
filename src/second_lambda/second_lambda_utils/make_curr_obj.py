from currency_codes import get_currency_by_code, CurrencyNotFoundError


def make_curr_obj(currency_dict):
    """
    This function:
        1) creates a Currency object that
            contains a currency name and
            returns that object.
        2) creates the currency object by
            getting the value of the
            'currency_code' key in the
            passed-in dictionary, passing
            that value to the imported
            get_currency_by_code() and
            returning the return value
            of that function (a
            Currency object).
        3) Gets called by second lambda
           utility function
           transform_to_dim_currency().

    Args:
        currency_dict: a dictionary that has key
         "currency_code", whose value is a code
         for a currency. The dictionary
         represents a row from the
         preprocessed currency dimension table.

    Returns:
        a Currency object that contains (among
        other information) the name of a currency.

    """

    err_Msg = "Invalid currency code provided"

    code = currency_dict["currency_code"]

    try:
        curr_obj = get_currency_by_code(code)
        return curr_obj

    except CurrencyNotFoundError as e:
        raise RuntimeError(err_Msg) from e
