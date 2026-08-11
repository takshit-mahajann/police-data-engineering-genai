from api_client import fetch_data


query = """
SELECT id, date_time, type
LIMIT 10
"""

data = fetch_data(query)

print("Records received:", len(data))

for record in data:
    print(record)