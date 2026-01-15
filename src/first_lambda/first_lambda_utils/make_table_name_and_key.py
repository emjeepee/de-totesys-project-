



def make_table_name_and_key(resp_dict):
    """
    This function:
        1) extracts an S3 key from 
        a dictionary that is the 
        response to a call to 
        S3_client.list_objects_v2(),
        where S3_client is a boto3 
        S3 client.

        2) extracts a table name from 
        the extracted key

    Args:
        resp_dict: a dictionary that
        is the response to a call to 
        S3_client.list_objects_v2(),
        where S3_client is a boto3 
        S3 client. It looks like 
        this:
         Looks like this:
        {
            'IsTruncated': True|False,
            'Contents': [
                    {
                  'Key': 'string',  # objects of interest are stored
                                  # under this key, which looks
                                  # like this:
                                  # '<table_name_here>/<timestamp_here>.json'

                'LastModified': datetime(2015, 1, 1),
                etc
                     },
                       etc (one such dict for each object)
                         ]
            'Name': 'string',       # the bucket name
            'Prefix': 'string',     # eg 'design' in key
                                        # 'design/<timestamp-here>.json'
            'KeyCount': 123,
            'MaxKeys': 123,
            etc (other keys)
        }


    Returns:
        a list containing a table 
        key and a table name
       
    
    """


    # Get the list of keys
    # under which the 
    # ingestion bucket stores 
    # versions of the table:
    keys_list = [dict["Key"] for dict in resp_dict.get("Contents")]
    # ['design/2025-06-02_22-17-19-2513.json',
    # 'design/2025-05-29_22-17-19-2513.json', etc]

    # Get the key for the 
    # latest table:
    latest_table_key = sorted(keys_list)[-1]
    # 'design/2025-06-02_22-17-19-2513.json'

    # get the table name from 
    # the key:
    table_name = latest_table_key.split("/")[0]


    return [latest_table_key, table_name]