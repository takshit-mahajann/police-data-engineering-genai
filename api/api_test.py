import requests
import time
from datetime import datetime, timezone


# ============================================================
# CONFIGURATION
# ============================================================

API_URL = "https://data.cambridgema.gov/api/v3/views/3gki-wyrb/query.json"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

TIMEOUT = 30
MAX_RETRIES = 3


# ============================================================
# 1. BASIC API REQUEST
# ============================================================

def call_api(query):
    """
    Send a query to the Cambridge Police API.

    Learning concepts:
    - HTTP POST
    - URL
    - headers
    - JSON request body
    - timeout
    - status code
    - response
    """

    payload = {
        "query": query
    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            print("\n" + "=" * 60)
            print(f"API REQUEST - Attempt {attempt}")
            print("=" * 60)

            start_time = time.time()

            response = requests.post(
                API_URL,
                headers=HEADERS,
                json=payload,
                timeout=TIMEOUT
            )

            elapsed_time = time.time() - start_time

            print("Status code :", response.status_code)
            print("Response time:", round(elapsed_time, 3), "seconds")
            print("Response size:", len(response.content), "bytes")

            # Raise exception for 4xx/5xx responses
            response.raise_for_status()

            # Convert JSON response into Python object
            data = response.json()

            print("Response type:", type(data).__name__)
            print("Records received:", len(data))

            return data

        except requests.exceptions.Timeout:
            print("ERROR: Request timed out.")

        except requests.exceptions.ConnectionError:
            print("ERROR: Could not connect to the API.")

        except requests.exceptions.HTTPError as error:
            print("HTTP ERROR:", error)

            # 429 = too many requests
            if response.status_code == 429:
                print("Rate limit/throttling detected.")

        except requests.exceptions.RequestException as error:
            print("REQUEST ERROR:", error)

        except ValueError:
            print("ERROR: Response was not valid JSON.")

        if attempt < MAX_RETRIES:
            wait_time = attempt * 2

            print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print("API request failed after all retries.")

    return None


# ============================================================
# 2. DISPLAY RECORDS
# ============================================================

def display_records(data, number=5):
    """
    Display a few records.

    Learning concepts:
    - Python lists
    - Python dictionaries
    - JSON parsing
    """

    if not data:
        print("\nNo records returned.")
        return

    print("\n" + "=" * 60)
    print(f"FIRST {min(number, len(data))} RECORDS")
    print("=" * 60)

    for index, record in enumerate(data[:number], start=1):

        print(f"\nRecord {index}")

        for key, value in record.items():
            print(f"{key}: {value}")


# ============================================================
# 3. INSPECT SCHEMA
# ============================================================

def inspect_schema(data):
    """
    Inspect fields returned by the API.

    Learning concepts:
    - Schema
    - dictionary keys
    - fields
    """

    if not data:
        print("Cannot inspect schema: no data.")
        return

    print("\n" + "=" * 60)
    print("SCHEMA")
    print("=" * 60)

    first_record = data[0]

    for field, value in first_record.items():

        print(
            f"{field:15} | "
            f"Python type: {type(value).__name__:10} | "
            f"Example: {value}"
        )


# ============================================================
# 4. VALIDATE REQUIRED COLUMNS
# ============================================================

def validate_schema(data):
    """
    Check whether required dataset columns exist.

    Learning concepts:
    - Data quality
    - Schema validation
    """

    required_columns = [
        "id",
        "date_time",
        "type",
        "subtype",
        "location",
        "description",
        "last_updated"
    ]

    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION")
    print("=" * 60)

    if not data:
        print("FAILED: No data returned.")
        return False

    actual_columns = set(data[0].keys())

    missing_columns = [
        column
        for column in required_columns
        if column not in actual_columns
    ]

    if missing_columns:

        print("FAILED")
        print("Missing columns:", missing_columns)

        return False

    print("PASSED: All required columns exist.")

    return True


# ============================================================
# 5. CHECK DUPLICATE IDS
# ============================================================

def check_duplicates(data):
    """
    Check duplicate police incident IDs.

    Learning concepts:
    - Primary key
    - uniqueness
    - data quality
    - deduplication
    """

    print("\n" + "=" * 60)
    print("DUPLICATE ID CHECK")
    print("=" * 60)

    if not data:
        print("No data to check.")
        return

    ids = [
        record.get("id")
        for record in data
        if record.get("id") is not None
    ]

    total_ids = len(ids)
    unique_ids = len(set(ids))

    print("Total IDs :", total_ids)
    print("Unique IDs:", unique_ids)

    if total_ids == unique_ids:
        print("PASSED: No duplicate IDs found.")
    else:
        print("WARNING: Duplicate IDs found.")


# ============================================================
# 6. CHECK NULL VALUES
# ============================================================

def check_nulls(data):
    """
    Check important columns for null/missing values.

    Learning concepts:
    - NULL values
    - data quality
    """

    print("\n" + "=" * 60)
    print("NULL VALUE CHECK")
    print("=" * 60)

    important_columns = [
        "id",
        "date_time",
        "type"
    ]

    if not data:
        print("No data to check.")
        return

    for column in important_columns:

        null_count = sum(
            1
            for record in data
            if record.get(column) in [None, ""]
        )

        print(f"{column}: {null_count} null/empty values")


# ============================================================
# 7. TEST BASIC SELECT + LIMIT
# ============================================================

def test_limit():

    print("\n\nTEST: SELECT + LIMIT")

    query = """
    SELECT *
    LIMIT 10
    """

    data = call_api(query)

    if data:
        print(f"Received {len(data)} records.")


# ============================================================
# 8. TEST SELECTING SPECIFIC COLUMNS
# ============================================================

def test_select_columns():

    print("\n\nTEST: SELECT SPECIFIC COLUMNS")

    query = """
    SELECT id, date_time, type, subtype, location
    LIMIT 10
    """

    data = call_api(query)

    display_records(data, 3)


# ============================================================
# 9. TEST FILTERING
# ============================================================

def test_filter():

    print("\n\nTEST: WHERE FILTER")

    query = """
    SELECT id, date_time, type, subtype
    WHERE type = 'ARREST'
    LIMIT 10
    """

    data = call_api(query)

    display_records(data, 5)


# ============================================================
# 10. TEST SORTING
# ============================================================

def test_sorting():

    print("\n\nTEST: ORDER BY")

    query = """
    SELECT id, date_time, type, subtype
    ORDER BY date_time DESC
    LIMIT 10
    """

    data = call_api(query)

    display_records(data, 5)


# ============================================================
# 11. TEST TIMESTAMP FILTER
# ============================================================

def test_timestamp_filter():

    print("\n\nTEST: TIMESTAMP FILTER")

    query = """
    SELECT id, date_time, last_updated, type
    WHERE last_updated > '2025-03-10T00:00:00'
    ORDER BY last_updated ASC
    LIMIT 10
    """

    data = call_api(query)

    display_records(data, 5)


# ============================================================
# 12. COMPARE date_time AND last_updated
# ============================================================

def compare_timestamps():

    print("\n\nTEST: date_time VS last_updated")

    query = """
    SELECT id, date_time, last_updated, type
    ORDER BY last_updated DESC
    LIMIT 10
    """

    data = call_api(query)

    if not data:
        return

    for record in data:

        print(
            f"ID: {record.get('id')} | "
            f"Incident: {record.get('date_time')} | "
            f"Updated: {record.get('last_updated')}"
        )


# ============================================================
# 13. TEST PAGINATION
# ============================================================

def test_pagination():

    print("\n\nTEST: PAGINATION")

    page_size = 10

    for page_number in [1, 2, 3]:

        print(f"\nFetching page {page_number}")

        query = f"""
        SELECT id, date_time, type
        ORDER BY date_time ASC
        LIMIT {page_size}
        OFFSET {(page_number - 1) * page_size}
        """

        data = call_api(query)

        if data:

            print(
                f"Page {page_number}: "
                f"{len(data)} records"
            )


# ============================================================
# 14. TEST EMPTY RESPONSE
# ============================================================

def test_empty_response():

    print("\n\nTEST: EMPTY RESPONSE")

    query = """
    SELECT *
    WHERE date_time > '2099-01-01T00:00:00'
    """

    data = call_api(query)

    if data is not None and len(data) == 0:

        print(
            "SUCCESS: API worked but "
            "returned zero records."
        )

    elif data is not None:

        print("Unexpected records returned.")


# ============================================================
# 15. TEST DATA TYPE / TIMESTAMP PARSING
# ============================================================

def test_timestamp_parsing(data):

    print("\n\nTEST: TIMESTAMP PARSING")

    if not data:
        print("No records.")
        return

    for record in data[:5]:

        timestamp = record.get("date_time")

        if timestamp:

            try:

                parsed = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )

                print(
                    timestamp,
                    "→",
                    parsed,
                    "| Type:",
                    type(parsed).__name__
                )

            except ValueError:

                print(
                    "Invalid timestamp:",
                    timestamp
                )


# ============================================================
# 16. TEST RESPONSE HEADERS
# ============================================================

def test_headers():

    print("\n\nTEST: RESPONSE HEADERS")

    query = """
    SELECT id
    LIMIT 1
    """

    try:

        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"query": query},
            timeout=TIMEOUT
        )

        print("Status:", response.status_code)

        print("\nImportant response headers:")

        for key, value in response.headers.items():

            if key.lower() in [
                "content-type",
                "content-length",
                "server",
                "date"
            ]:

                print(f"{key}: {value}")

    except requests.exceptions.RequestException as error:

        print("Request failed:", error)


# ============================================================
# 17. MAIN PROGRAM
# ============================================================

def main():

    print("\n")
    print("=" * 60)
    print("CAMBRIDGE POLICE API TESTING")
    print("=" * 60)

    # --------------------------------------------------------
    # Basic API test
    # --------------------------------------------------------

    query = """
    SELECT *
    LIMIT 10
    """

    data = call_api(query)

    if data is None:

        print("\nAPI test failed.")
        return

    # --------------------------------------------------------
    # Basic inspection
    # --------------------------------------------------------

    display_records(data, 2)

    inspect_schema(data)

    validate_schema(data)

    check_duplicates(data)

    check_nulls(data)

    test_timestamp_parsing(data)

    # --------------------------------------------------------
    # Query tests
    # --------------------------------------------------------

    test_limit()

    test_select_columns()

    test_filter()

    test_sorting()

    test_timestamp_filter()

    compare_timestamps()

    # --------------------------------------------------------
    # Pagination
    # --------------------------------------------------------

    test_pagination()

    # --------------------------------------------------------
    # Empty response
    # --------------------------------------------------------

    test_empty_response()

    # --------------------------------------------------------
    # Headers
    # --------------------------------------------------------

    test_headers()

    print("\n")
    print("=" * 60)
    print("API TESTING COMPLETED")
    print("=" * 60)


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()