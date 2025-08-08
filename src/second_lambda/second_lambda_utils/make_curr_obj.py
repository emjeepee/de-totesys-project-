from currency_codes import get_currency_by_code, Currency



def make_curr_obj(currency_dict):
        """
        This function:
            creates a Currency object that will
             contain a currency name.
        
        Args:
            currency_dict: a dictionary that has key 
            "currency_code", whose value is a code 
            for a currency.

        Returns:
            a Currency object that contains (among
            other information) the name of a currency.            

        """
        curr_obj = get_currency_by_code(currency_dict.get("currency_code"))
        return curr_obj