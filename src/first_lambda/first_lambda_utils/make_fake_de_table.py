from datetime import datetime


def make_fake_de_table():
    """
    This function:
        makes a fake design
        table in the form of a
        list of dictionaries.

    Args:
        None

    Returns:
        A list of dictionaries
        that represents the
        design table from the
        totesys database.

    """

    base_day = datetime(2025, 11, 13)
    # td_1_day = timedelta(days=1)

    file_names = [
        "steel-20240925-qsbh.json",
        "rubber-20240419-jltq.json",
        "cotton-20240206-tukd.json",
        "copper-20240901-gyw8.json",
        "plastic-20241121-t5de.json",
        "silicon-20240927-nbtx.json",
        "frozen-20250227-qec3.json",
        "wooden-20240923-ogst.json",
        "metal-20240407-3vwp.json",
        "iron-20230723-mgsr.json",
    ]

    design_names = [
        "steel",
        "rubber",
        "cotton",
        "copper",
        "plastic",
        "silicon",
        "frozen",
        "wooden",
        "metal",
        "iron",
    ]

    de_table = [
        {
            "design_id": i,
            "created_at": base_day,
            "design_name": design_names[i - 1],
            "file_location": "/usr/ports",
            "file_name": file_names[i - 1],
            "last_updated": base_day,
        }
        for i in range(1, 11)  # 1, 2, 3... 10
    ]

    return de_table
