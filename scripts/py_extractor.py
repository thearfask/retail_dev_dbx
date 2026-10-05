import requests

response = requests.get(
    "https://dummyjson.com/carts",
    params={"limit": 5, "skip": 0},
    timeout=30
)
response.raise_for_status()

payload = response.json()

def fetch_all_pages(entity, page_size=20):
    # 1. Start skip at 0 and create an empty records list.
    # 2. Request a page using limit and skip.
    # 3. Check the HTTP response and parse JSON.
    # 4. Extend records with payload[entity].
    # 5. Advance skip by the number of records received.
    # 6. Stop when skip reaches total.
    # 7. Raise an error if an empty page arrives before total.
    # 8. Return records.
    pass