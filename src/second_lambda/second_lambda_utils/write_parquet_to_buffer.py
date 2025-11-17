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
    # in RAM that acts like a file
    buffer = BytesIO()

    # The if statement below takes
    # care of the problem of testing
    # this function with a mock 
    # tmp_path (ie when the file 
    # path is not real):
    if os.path.exists(tmp_path):
        # Open the temp file in 
        # read-binary ('rb') mode and 
        # write it to the buffer:
        with open(tmp_path, 'rb') as f:
            buffer.write(f.read())
            buffer.seek(0)            

        # permanently delete the 
        # temporary Parquet file 
        # at path tmp_path:          
        os.remove(tmp_path)
        print(f"MY_INFO >>>>> In function write_parquet_to_buffer(). About to return the buffer.")
        return buffer
    
    