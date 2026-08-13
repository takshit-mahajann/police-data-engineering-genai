### 13/08/2026
# Cambridge Police Data Engineering Project — Full History

## Phase 0 — Project idea and architecture

We started with the idea of building a **Data Engineering pipeline around the Cambridge Police public API**.

The overall goal became:

```text
Cambridge Police API
        ↓
API Testing
        ↓
API Client
        ↓
Incremental Ingestion
        ↓
Raw JSON
        ↓
PySpark
        ↓
Curated / Analytics Parquet
        ↓
Airflow
        ↓
PostgreSQL
        ↓
Flask/API
        ↓
Chatbot
```

Your collaborator is working on the **chatbot side**, while your main responsibility is the **data pipeline**.

---

# Phase 1 — API understanding and testing

## 1. Learned what an API is

We first discussed:

* API URL
* HTTP methods
* GET vs POST
* request parameters
* JSON response
* status codes
* API limits
* pagination
* rate limiting
* authentication
* error handling
* incremental API requests

The Cambridge Police dataset was obtained through the Cambridge Open Data API.

---

## 2. Tested the API manually

We worked with the Cambridge Police endpoint and tested queries such as:

```python
import requests

url = "https://data.cambridgema.gov/api/v3/views/3gki-wyrb/query.json"

payload = {
    "query": "SELECT * LIMIT 10"
}

response = requests.post(
    url,
    json=payload,
    timeout=30
)

print(response.status_code)
print(response.text)
```

You learned what each part does:

```text
requests.post()
     ↓
URL
     ↓
JSON payload
     ↓
API
     ↓
HTTP response
     ↓
JSON data
```

---

# Phase 2 — API test module

## 3. Created `api_test.py`

The goal was not just to make an API call, but to use the file to learn basic API testing.

We covered things such as:

```text
Status code
Response time
Response JSON
Required fields
Record count
Duplicate IDs
Missing fields
Invalid responses
```

The idea was:

```text
API
 ↓
api_test.py
 ↓
Is API behaving correctly?
```

---

## 4. Created API conclusion testing

We also discussed having an `api_conclusion_test`/conclusion section to summarize whether the API tests passed.

The API testing stage was essentially:

```text
Connection test
      ↓
Status test
      ↓
JSON test
      ↓
Schema test
      ↓
Data quality test
      ↓
Conclusion
```

---

# Phase 3 — API client

## 5. Created `api_client.py`

We separated the actual API communication from the ingestion logic.

Conceptually:

```text
api_client.py
       │
       └── talks to Cambridge API
```

while:

```text
ingestion.py
       │
       └── controls the ingestion pipeline
```

This separation was important because later the ingestion process could call:

```python
fetch_data(query)
```

instead of having HTTP code everywhere.

---

# Phase 4 — Incremental ingestion

## 6. Learned incremental loading

This was one of the important Data Engineering concepts.

Instead of repeatedly downloading:

```text
ALL records
```

we implemented:

```text
Previous checkpoint
       ↓
API query
       ↓
Only newer records
       ↓
New checkpoint
```

The query became:

```sql
SELECT *
WHERE last_updated > 'previous_timestamp'
ORDER BY last_updated ASC
LIMIT 100
```

---

## 7. Created `checkpoint.json`

The checkpoint stores the last successfully processed timestamp.

Example:

```json
{
  "last_updated": "2025-10-31T14:00:05.000"
}
```

The important rule we implemented was:

```text
Fetch
 ↓
Validate
 ↓
Save raw data
 ↓
Update checkpoint
```

The checkpoint should **not** be updated before successful processing.

---

# Phase 5 — Batch ingestion

## 8. Improved ingestion to process batches

Instead of one API request, `ingestion.py` keeps requesting:

```text
LIMIT 100
```

until no records remain.

Conceptually:

```text
Batch 1 → 100
Batch 2 → 100
Batch 3 → 100
...
Batch N → 100
Batch N+1 → 0
```

We also added:

* batch numbering
* total record count
* validation
* raw batch storage
* checkpoint updates
* ingestion summary

---

## 9. Raw data organization

We changed raw storage from one file being overwritten to separate batch files.

Example:

```text
raw/
└── 2026-08-12/
    ├── batch_1_....json
    ├── batch_2_....json
    ├── batch_3_....json
    └── ...
```

This was important because otherwise every ingestion run could overwrite previous raw data.

---

# Phase 6 — Ingestion issue and fix

## 10. Encountered missing column

During batch 46 we got:

```text
ValueError: Missing columns: {'description'}
```

The API returned a record without `description`.

We recognized that requiring every record to have every field was too strict.

The ingestion validation was therefore treated as a **data-quality boundary**, rather than assuming the API is perfectly consistent.

---

# Phase 7 — Large historical ingestion

## 11. Ran the ingestion successfully

You eventually ingested approximately:

```text
96 batches
```

with 100 records per batch in the historical run.

So you had a substantial raw dataset ready for Spark processing.

---

# Phase 8 — GitHub planning

## 12. Decided what belongs in GitHub

We discussed that you **should not push the entire raw dataset**.

The repository should contain:

```text
Code
Configuration
environment.yml
README
.gitignore
```

but not:

```text
raw JSON
generated Parquet
Conda environment
checkpoint state
```

The intended `.gitignore` includes:

```gitignore
ingestion/raw/
ingestion/checkpoint.json
sparkModule/output/
.env
__pycache__/
```

The reason is:

```text
GitHub = source code
Raw/processed data = generated data
```

Your collaborator can reproduce the data pipeline instead of downloading your local generated files.

---

# Phase 9 — Conda environment

## 13. Created `policeDE`

Your environment was designed around:

```text
Python 3.10.19
requests
pandas
python-dotenv
PySpark
```

You had:

```yaml
name: policeDE

channels:
  - conda-forge
  - defaults

dependencies:
  - python=3.10.19
  - pip
  - requests
  - pandas
  - python-dotenv
  - pyspark=4.2.0
```

---

# Phase 10 — PySpark module

## 14. Created `sparkModule`

We decided to keep Spark work separate:

```text
sparkModule/
```

The intended structure became:

```text
sparkModule/
├── schema.py
├── cleaning.py
├── quality.py
├── aggregations.py
├── storage.py
├── window.py
├── transform.py
└── output/
```

---

# Phase 11 — First Spark problem

## 15. Raw JSON initially produced `_corrupt_record`

We encountered:

```text
DataFrame Schema:

root
 |-- _corrupt_record: string
```

and then:

```text
UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN
```

We fixed the raw JSON reading approach and introduced an **explicit schema** rather than relying on Spark to infer the structure incorrectly.

---

# Phase 12 — `schema.py`

## 16. Created explicit Spark schema

We separated the schema into:

```text
schema.py
```

This gave Spark the expected structure of the Cambridge Police records.

Main fields:

```text
id
date_time
type
subtype
location
description
last_updated
```

plus the API metadata fields where applicable.

---

# Phase 13 — `cleaning.py`

## 17. Created the cleaning module

The cleaning pipeline was designed to:

```text
Remove API metadata
       ↓
Trim strings
       ↓
Standardize type
       ↓
Standardize location
       ↓
Handle missing description
       ↓
Remove missing critical fields
       ↓
Remove duplicates
       ↓
Create time columns
```

We added:

```text
year
month
day
hour
day_of_week
```

These became useful for analytics and storage partitioning.

---

# Phase 14 — `quality.py`

## 18. Added data-quality checks

We separated quality validation from cleaning.

The purpose was to ensure:

```text
IDs are valid
timestamps exist
required data exists
duplicates are controlled
```

The pipeline should stop rather than silently write bad data.

---

# Phase 15 — `aggregations.py`

## 19. Created analytics aggregations

We implemented aggregation logic for things such as:

```text
Incidents by type
Incidents by subtype
Incidents by location
Incidents by month
Incidents by hour
```

This introduced:

```python
groupBy()
count()
agg()
```

and related Spark DataFrame operations.

---

# Phase 16 — `storage.py`

## 20. Added Parquet output

Initially the Spark pipeline successfully:

```text
Read JSON
 ↓
Clean
 ↓
Quality
 ↓
Aggregate
```

but then failed while writing Parquet on Windows.

The error was:

```text
HADOOP_HOME and hadoop.home.dir are unset
```

---

# Phase 17 — Windows Spark/Hadoop issue

## 21. Diagnosed the problem

You were **not running a Hadoop cluster**.

The issue was that Spark's filesystem layer uses Hadoop libraries internally, and Windows required the Hadoop filesystem support.

Your versions were:

```text
Spark 4.2.0
Hadoop 3.5.0
Java 17.0.12
```

We checked:

```text
HADOOP_HOME
```

and:

```text
where winutils
```

and found:

```text
HADOOP_HOME → empty
winutils → not found
```

---

# Phase 18 — Moved Spark execution to WSL2

## 22. Discovered WSL2 was already installed

You checked:

```text
wsl --status
```

and:

```text
wsl --list --verbose
```

and had:

```text
Ubuntu          Stopped    2
docker-desktop  Stopped    2
```

So there was no need to install WSL.

---

# Phase 19 — WSL Java

## 23. Installed Java 17 in WSL

Windows Java and WSL Java are separate.

We installed:

```text
OpenJDK 17.0.19
```

and configured:

```text
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Verified:

```text
java --version
```

successfully.

---

# Phase 20 — Miniconda in WSL

## 24. Installed Miniconda

The first Miniconda installer download was corrupted:

```text
md5sum mismatch
Could not load PyInstaller's embedded PKG archive
```

We removed it, downloaded again, and successfully installed Miniconda.

Your WSL Conda version became:

```text
conda 26.5.3
```

---

# Phase 21 — Linux `policeDE`

## 25. Recreated the environment inside WSL

We used your existing:

```text
environment.yml
```

to create a separate Linux:

```text
policeDE
```

environment.

Important:

```text
Windows policeDE
        ≠
WSL policeDE
```

They are separate environments.

---

# Phase 22 — Spark finally ran successfully

## 26. Ran `transform.py` in WSL

This was the important milestone.

The same pipeline that failed on Windows successfully ran in WSL:

```text
Raw JSON
   ↓
Schema
   ↓
Cleaning
   ↓
Quality
   ↓
Aggregations
   ↓
Parquet
```

So the Windows Hadoop filesystem issue was effectively bypassed by running Spark in Linux/WSL.

---

# Phase 23 — Storage partitioning

## 27. Added Parquet partitioning

We changed `storage.py` to:

```python
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(...)
```

So curated data is organized like:

```text
output/
└── curated/
    ├── year=2025/
    │   ├── month=1/
    │   ├── month=2/
    │   └── ...
    └── year=2026/
        ├── month=1/
        └── ...
```

This introduced the concept of **storage partitioning**.

We also explicitly distinguished:

```text
Storage partitionBy
        vs
Window partitionBy
```

---

# Where we are RIGHT NOW

The Spark module is currently at:

```text
Schema                  ✅
Cleaning                ✅
Data Quality            ✅
Aggregations            ✅
Parquet                 ✅
Storage partitioning    ✅
WSL Spark environment   ✅

Window Functions        → CURRENT
Joins                   → NEXT
Spark execution model   → NEXT
Shuffle                 → NEXT
Performance             → NEXT
Incremental Spark       → NEXT
Testing                 → NEXT
```

---

# The complete Spark target

We decided that `sparkModule` should teach and implement:

```text
1. Reading JSON
2. Explicit schemas
3. Data cleaning
4. Data quality
5. DataFrame transformations
6. Aggregations
7. Window functions
8. Joins
9. Lazy evaluation
10. Transformations vs actions
11. Narrow vs wide transformations
12. Shuffle
13. Spark partitions
14. repartition()
15. coalesce()
16. Parquet
17. Partition pruning
18. Caching/persisting
19. Spark execution/performance
20. Incremental Spark processing
21. Testing
```

Then:

```text
SparkModule
     ↓
Airflow
     ↓
PostgreSQL
     ↓
Flask/API
     ↓
Chatbot
```

---

# Current project status

So if you come back later and say **"continue the police project"**, the exact point is:

```text
Cambridge Police API
        ↓
API testing              ✅
        ↓
api_client.py            ✅
        ↓
Incremental ingestion    ✅
        ↓
checkpointing             ✅
        ↓
~96 raw batches           ✅
        ↓
GitHub structure          ✅
        ↓
PySpark environment       ✅
        ↓
schema.py                 ✅
        ↓
cleaning.py               ✅
        ↓
quality.py                ✅
        ↓
aggregations.py           ✅
        ↓
storage.py                ✅
        ↓
Partitioned Parquet       ✅
        ↓
WINDOW FUNCTIONS         ← WE STOPPED HERE
        ↓
Joins
        ↓
Spark performance
        ↓
Incremental Spark
        ↓
Testing
        ↓
AIRFLOW
```

**The next lesson/implementation is Window Functions from the beginning**, starting with `Window.partitionBy()`, `orderBy()`, and `lag()`.
