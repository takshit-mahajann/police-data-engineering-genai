**Cambridge Daily Police Log dataset**

![Image](https://images.openai.com/static-rsc-4/xYJmCYjhf7TlGPCocOje0LjFSIl27j_Nijyz8h-FkPfCAe5fweo6fp-CzB9BuKiJpLQZrA2xd9JkstWZhb5Fe_EF9PwUr_8Wv0B-xLvyvCy6lRRfbxxGa_xGoqudXrdYDqZ15OqCaVaJ5VQWg7g5zBeXM1QrND1j3xjjz_Ov97GtfvH06a6Ur950LVWWT5bn?purpose=fullsize)

## 1. What is this dataset?

The **Daily Police Log** is an official open-data dataset from the **Cambridge Police Department (CPD)** in Cambridge, Massachusetts.

[Official Daily Police Log dataset](https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb?utm_source=chatgpt.com)

Each row represents a **distinct call for service or police report**. It covers both criminal and non-criminal public-safety activity. ([City of Cambridge][1])

Examples include:

* theft/law enforcement incidents
* traffic incidents
* shoplifting
* burglary
* arrests
* wellness checks
* threats
* miscellaneous reports
* community calls
---

# 2. Why this dataset?
> __**It changes over time.**__

The official portal currently lists the estimated update frequency as **daily**, with logs typically updated Monday–Friday for previous business day(s). ([City of Cambridge][1])

The dataset's changelog also shows actual incremental updates. For example, the portal records recent changes where new rows were inserted and no rows deleted. ([City of Cambridge][1])

The formation of realistic scenario:

```text
              Cambridge Police
                     │
                     ▼
              New daily logs
                     │
                     ▼
               Open Data API
                     │
                     ▼
                  Airflow
                     │
                     ▼
                  PySpark
                     │
                     ▼
                PostgreSQL
                     │
              ┌──────┴──────┐
              ▼             ▼
          Dashboard       Chatbot
```
---

# 3. How much historical data?

The open-data version was announced in **May 2025**.

Cambridge says the dataset initially contained records from **January 2025 onward**, with work underway to expand historical coverage back to 2013. ([City of Cambridge][2])

So don't assume that the open-data dataset contains the entire historical police-log archive.

---

# 4. Dataset metadata

The current official portal shows:

| Property                   | Details                                                |
| -------------------------- | ------------------------------------------------------ |
| Dataset                    | Daily Police Log                                       |
| Provider                   | Cambridge Police Department                            |
| Category                   | Public Safety                                          |
| Tags                       | crime, police                                          |
| Dataset ID                 | `3gki-wyrb`                                            |
| Rows                       | Police Log Entries                                     |
| Columns                    | **7**                                                  |
| Row identifier             | `ID`                                                   |
| Estimated update frequency | **Daily (Monday To Friday)**                                              |
| License                    | Open Data Commons Public Domain Dedication and License |
| Created                    | February 19, 2025                                      |


---

# 5. The 7 columns

This is the **actual schema currently documented by Cambridge**. ([City of Cambridge][1])

| Column       | API field      | Type               | Purpose                                   |
| ------------ | -------------- | ------------------ | ----------------------------------------- |
| Date & Time  | `date_time`    | Floating Timestamp | When police response/report was initiated |
| ID           | `id`           | Text               | Unique incident/report identifier         |
| Type         | `type`         | Text               | Broad report classification               |
| Subtype      | `subtype`      | Text               | More specific classification              |
| Location     | `location`     | Text               | Street/intersection                       |
| Description  | `description`  | Text               | Brief incident narrative                  |
| Last Updated | `last_updated` | Floating Timestamp | Technical ETL update timestamp            |

---

# 6. `id`

Example:

```text
26006024
```

This identifies a particular police log entry.

Cambridge defines it as:

> A unique identifier assigned to each police report or incident log entry. ([City of Cambridge][1])


### Why do you need this?

For deduplication.

Imagine:

```text
API Run 1

26006024
26006025
26006026
```

Next run:

```text
26006026
26006027
26006028
```

You don't want:

```text
26006026
26006026
```

in PostgreSQL.

So:

```text
id
 ↓
unique constraint
 ↓
deduplication
```

---

# 7. `date_time`

This is the actual incident/report time.

For example, a police log may show:

```text
7/11/2026 7:45 AM
```

Cambridge defines this as the date and time when the police response or report was initiated. ([City of Cambridge][1])

This is our **business timestamp**.

We can derive:

```text
date
hour
day_of_week
month
year
weekend/weekday
```

using PySpark.

For example:

```text
date_time
    ↓
2026-07-11 07:45
    │
    ├── date = 2026-07-11
    ├── hour = 7
    ├── day = Saturday
    ├── month = July
    └── weekend = TRUE
```

This gives excellent dashboard dimensions.

---

# 8. `type`

This is the **broad category**.

The official documentation gives `"INCIDENT"` as an example. ([City of Cambridge][1])

Your analytics can ask:

```text
How many INCIDENT records?
How many TRAFFIC records?
How many ARREST records?
```

This becomes a dashboard dimension.

---

# 9. `subtype`

This is where the dataset becomes much more useful.

It provides a more specific classification and can reference Massachusetts General Laws.

Examples from Cambridge's actual police logs include things like:

```text
SHOPLIFTING BY ASPORTATION C266 S30A
LARCENY FROM BUILDING C266 S20
BURGLARY, UNARMED C266 S15
THREAT TO COMMIT CRIME C275 S2
```

Actual CPD daily logs demonstrate these kinds of records. ([City of Cambridge][3])

So we can build analytics like:

```text
Top 10 incident subtypes
```

or:

```text
Shoplifting trend
```

or:

```text
Larceny incidents this month vs last month
```

---

# 10. `location`

This is **not an exact address**.

Cambridge intentionally limits location information to the street or block level to protect privacy. ([City of Cambridge][2])

For example:

```text
MASSACHUSETTS AVE
CAMBRIDGE ST
MAIN ST
BRATTLE ST
```

This allows us to perform:

```text
Top streets by incidents
Incident count by location
Location trends
```

But we shouldn't claim that the dataset provides exact incident coordinates.

---

# 11. `description`

This is a short narrative describing what happened.

Example from an actual CPD log:

> A resident reported their vehicle had been broken into...

The description is useful for the **GenAI component**, because the chatbot could potentially summarize narratives.

But be careful:

**Don't make the LLM blindly ingest every description.**

---

# 12. `last_updated` 
`last_updated` does **NOT** mean:

> "When the crime happened."

It represents the timestamp when the dataset's ETL process detected a new input file from the police department's reviewer. Cambridge explicitly warns that it reflects **technical update activity**, not when the incident occurred or when a human modified the record. ([City of Cambridge][1])

Therefore:

```text
date_time
     ↓
Business event time

last_updated
     ↓
Data pipeline / technical update time
```
---

# 13. The really interesting part: changelog

The dataset has a **changelog**.

For example, the official portal records updates such as:

```text
July 17
25 rows inserted
0 deleted

July 16
15 rows inserted
0 deleted

July 15
28 rows inserted
0 deleted
```

and other updates with additional inserted rows. ([City of Cambridge][1])

This is exactly the type of behavior you want to exploit.

Instead of pretending this is a static CSV:

```text
CSV → Spark → Dashboard
```

you can demonstrate:

```text
New data
   ↓
API
   ↓
Airflow detects/ingests
   ↓
Incremental processing
   ↓
PySpark
   ↓
PostgreSQL
   ↓
Dashboard changes
```

---

# 14. Privacy considerations

This is important if we're presenting this project publicly.

Cambridge says the dataset **does not include personally identifiable information** and does not publish exact addresses. ([City of Cambridge][2])

However, the narrative field can contain sensitive incident details.

Therefore our chatbot should have safeguards around:

```text
PII
sensitive descriptions
individual identification
```

The chatbot should primarily answer **aggregate analytical questions**.

For example:

✅

> "What were the top five incident categories this week?"

Better than:

❌

> "Tell me everything about the person involved in incident X."

---

# 15. What we can build from these 7 columns

You can turn the raw schema into analytical features.

### Raw

```text
id
date_time
type
subtype
location
description
last_updated
```

### PySpark-derived

```text
incident_date
incident_hour
day_of_week
month
year
is_weekend
```

Potentially:

```text
subtype_category
location_category
```

Then your PostgreSQL analytics layer could have:

```text
fact_police_incident
```

with dimensions such as:

```text
date
time
incident type
subtype
location
```

---

# 16. Dashboard possibilities
### KPI cards

```text
Total Incidents
Today's Incidents
This Week
This Month
Top Incident Type
Top Location
```

### Charts

```text
Incidents by Date
        ↓
line chart

Incidents by Type
        ↓
bar chart

Incidents by Hour
        ↓
histogram

Top Locations
        ↓
bar chart

Incident Subtypes
        ↓
bar chart

Weekday vs Weekend
        ↓
comparison
```

---

# 17. Chatbot questions
For example:

> **"How many incidents occurred this week?"**

> **"What is the most common incident type?"**

> **"Which street had the most reported incidents?"**

> **"Compare this week with last week."**

> **"What time of day has the most incidents?"**

> **"Show the trend of shoplifting incidents over the last month."**

The LLM generates a read-only SQL query against your analytics tables.

```text
User
 ↓
LLM
 ↓
SQL
 ↓
PostgreSQL
 ↓
Result
 ↓
LLM
 ↓
Natural-language answer
```

And because PostgreSQL is continuously updated by your Airflow pipeline, **the chatbot automatically works with new data**.

---

## 18. One correction to keep in mind

Earlier we talked about this as a "real-time" dataset.

I would **not call Cambridge's source itself real-time**.

The official maintenance plan says **daily**, and the city says logs are automatically added as they become available. ([City of Cambridge][2])

So your project should describe it as:

> **Automated near-current / daily incremental public-safety data pipeline**

rather than claiming:

> ❌ Real-time police data streaming.

Our **application** can still update automatically without the user refreshing the page, but that doesn't make the source itself real-time.

---

### The key takeaway for your project

This dataset gives you a very clean DE story:

**Source:** Cambridge Police Open Data
↓
**API:** structured machine-readable data
↓
**Incremental ingestion:** new/updated records
↓
**Airflow:** scheduling, retries, monitoring
↓
**PySpark:** cleaning + transformation + aggregation
↓
**PostgreSQL:** serving/analytics layer
↓
**Flask:** authentication + APIs
↓
**Dashboard:** automatically updates
↓
**GenAI:** natural-language analytics over the same database


[1]: https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb?utm_source=chatgpt.com "Daily Police Log | Open Data Portal | City of Cambridge"
[2]: https://www.cambridgema.gov/departments/opendata/news/2025/05/revampeddailypolicelognowavailableasopendata?utm_source=chatgpt.com "Revamped Daily Police Log Now Available as Open Data"
[3]: https://www.cambridgema.gov/departments/cambridgepolice/news/2026/07/07112026?utm_source=chatgpt.com "Daily Log for July 11, 2026"
