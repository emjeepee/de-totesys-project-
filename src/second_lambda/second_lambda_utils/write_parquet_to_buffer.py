import os
from io import BytesIO


def write_parquet_to_buffer(tmp_path):
    """
    This function:
        Checks whether a certain 
        path to a temporary 
        Parquet file exists. If 
        it does this function 
        makes a context in which 
        the file can be read and 
        then writes the file 
        contents to an IOBytes 
        buffer.
    
    Args:
        tmp_path: a string that is 
         a path to a temporary 
         Parquet file.        
    
    Returns:
        An IOBytes object containing
        the contents of the Parquet file. 
    """
 
    # Create an empty container 
    # in RAM (ie one that never 
    # touches the filesystem) 
    # that holds bytes and acts 
    # like a file:
    buffer = BytesIO()

    # The if statement below takes
    # care of the problem of 
    # testing this function with 
    # a mock tmp_path (ie when the 
    # file path is not real):
    if os.path.exists(tmp_path):
        # Open the file in path tmp_path
        # in read-binary ('rb') mode and 
        # write it to the buffer:
        with open(tmp_path, 'rb') as f:
            buffer.write(f.read())
            buffer.seek(0)            

        # permanently delete the 
        # temporary Parquet file 
        # at path tmp_path:          
        os.remove(tmp_path)

        return buffer
    
    