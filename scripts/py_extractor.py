import requests

response = requests.get(
    "https://dummyjson.com/carts",
    params={"limit": 5, "skip": 0},
    timeout=30
)
response.raise_for_status()

payload = response.json()

print("Total carts:", payload["total"])
print("Fetched carts:", len(payload["carts"]))
print(payload["carts"][0])