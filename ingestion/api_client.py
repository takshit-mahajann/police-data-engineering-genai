import requests


API_URL = "https://data.cambridgema.gov/api/v3/views/3gki-wyrb/query.json"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

TIMEOUT = 30


def fetch_data(query):
    payload = {
        "query": query
    }

    response = requests.post(
        API_URL,
        headers=HEADERS,
        json=payload,
        timeout=TIMEOUT
    )

    response.raise_for_status()

    return response.json()