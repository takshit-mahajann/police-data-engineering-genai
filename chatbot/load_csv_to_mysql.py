import pandas as pd
from sqlalchemy import create_engine

CSV_PATH = "/mnt/d/AmitTrivediProj/chatbot/Daily_Police_Log_20260819.csv"

DB_USER = "root"
DB_PASSWORD = "PoliceDE2026!"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "crime_chatbot"

df = pd.read_csv(CSV_PATH)

# Normalize column names to match the MySQL table
df = df.rename(columns={
    "Date & Time": "record_datetime",
    "ID": "id",
    "Type": "type",
    "Subtype": "subtype",
    "Location": "location",
    "Description": "description",
    "Last Updated": "last_updated",
})

# Parse the CSV's date format: "2025 Mar 06 01:07:00 AM"
df["record_datetime"] = pd.to_datetime(
    df["record_datetime"], format="%Y %b %d %I:%M:%S %p", errors="coerce"
)
df["last_updated"] = pd.to_datetime(
    df["last_updated"], format="%Y %b %d %I:%M:%S %p", errors="coerce"
)

# Data quality fix: some source rows have the description text duplicated
# inside the location field itself, separated by a long run of padding
# spaces (e.g. "INMAN ST" + 300 spaces + description again). Keep only
# the text before the first run of 2+ spaces, then hard-cap length as a
# safety net for any other anomalies.
df["location"] = (
    df["location"]
    .astype(str)
    .str.split(r"\s{2,}", n=1, regex=True)
    .str[0]
    .str.strip()
    .str.slice(0, 255)
)

print(f"Loaded {len(df)} rows from CSV")
print(f"Rows with unparseable record_datetime: {df['record_datetime'].isna().sum()}")
print(f"Rows with unparseable last_updated: {df['last_updated'].isna().sum()}")

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

df.to_sql("police_log", engine, if_exists="append", index=False, chunksize=1000)

print("Load complete.")

with engine.connect() as conn:
    result = conn.exec_driver_sql("SELECT COUNT(*) FROM police_log")
    print(f"Total rows now in police_log table: {result.scalar()}")
