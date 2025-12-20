from datetime import datetime


def make_fake_cp_table():

    base_day = datetime(2025, 11, 13)
    # td_1_day = timedelta(days=1)

    cp_table = [
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Fahey and Sons",
            "legal_address_id": 2,
            "commercial_contact": "Micheal Toy",
            "delivery_contact": "Mrs. Lucy Runolfsdottir",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Leannon, Predovic and Morar",
            "legal_address_id": 8,
            "commercial_contact": "Melba Sanford",
            "delivery_contact": "Jean Hane III",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Armstrong Inc",
            "legal_address_id": 3,
            "commercial_contact": "Jane Wiza",
            "delivery_contact": "Myra Kovacek",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Kohler Inc",
            "legal_address_id": 7,
            "commercial_contact": "Taylor Haag",
            "delivery_contact": "Alfredo Cassin II",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Frami, Yundt and Macejkovic",
            "legal_address_id": 1,
            "commercial_contact": "Homer Mitchell",
            "delivery_contact": "Ivan Balistreri",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Mraz LLC",
            "legal_address_id": 6,
            "commercial_contact": "Florence Casper",
            "delivery_contact": "Eva Upton",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Padberg, Lueilwitz and Johnson",
            "legal_address_id": 4,
            "commercial_contact": "Ms. Wilma Witting",
            "delivery_contact": "Christy O'Hara",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Grant - Lakin",
            "legal_address_id": 5,
            "commercial_contact": "Emily Orn",
            "delivery_contact": "Veronica Fay",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Price LLC",
            "legal_address_id": 10,
            "commercial_contact": "Sheryl Langworth",
            "delivery_contact": "Simon Schoen",
            "created_at": None,
            "last_updated": None,
        },
        {
            "counterparty_id": None,
            "counterparty_legal_name": "Bosco - Grant",
            "legal_address_id": 9,
            "commercial_contact": "Ed Halvorson",
            "delivery_contact": "Dewey Kuhic",
            "created_at": None,
            "last_updated": None,
        },
    ]

    for i in range(1, 11):  # 1, 2, 3... 10
        cp_table[i - 1]["counterparty_id"] = i
        cp_table[i - 1]["created_at"] = base_day
        cp_table[i - 1]["last_updated"] = base_day

    return cp_table
