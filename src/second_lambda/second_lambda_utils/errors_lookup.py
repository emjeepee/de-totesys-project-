# a lookup table for
# error messages that
# first lambda utlilities
# will employ when logging

err_0 = """
Error caught in read_from_s3()
while trying to read ingestion
bucket.
"""

err_1 = """
Error caught in
is_first_run_of_pipeline()
while trying to read the
ingestion bucket."""

err_2 = """
Error caught in
upload_to_s3()
while trying to write to
the processed bucket.
"""

err_3 = """
Error caught in get_latest_table()
while trying to list the objects
in the ingestion bucket.
"""

err_4 = """
Error caught in get_latest_table()
while trying to get an object
from the ingestion bucket.
"""


errors_lookup = {
    "err_0": err_0,
    "err_1": err_1,
    "err_2": err_2,
    "err_3": err_3,
    "err_4": err_4,
}
