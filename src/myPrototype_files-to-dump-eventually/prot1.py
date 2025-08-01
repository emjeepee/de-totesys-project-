from datetime import datetime, UTC

# First create a timestamp string for the current time,
    # like this: "2025-06-04T08:28:12":

def test_fn():    
    now_ts_with_ms = datetime.now(UTC).isoformat()
    now_ts = now_ts_with_ms[:-13]
    print(f'This timestamp is: {now_ts}')

test_fn()