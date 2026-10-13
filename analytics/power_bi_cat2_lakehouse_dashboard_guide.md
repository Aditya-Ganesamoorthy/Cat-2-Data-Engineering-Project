# CAT-2 Week 3: Power BI Lakehouse Dashboard Integration Guide

This guide details the exact step-by-step procedure to connect Power BI Desktop to the Trino-served Delta Lake tables and construct the **CAT-2 Lakehouse Dashboard** (`Cat-2 Lakehouse Dashboard.pbix`).

---

## 1. Trino Serving Layer Connection Settings

Trino is running as a containerized query engine (`de-trino`) in Docker Compose, connected directly to the Delta Lake storage layer via the `delta_lake` connector.

| Parameter | Value |
| :--- | :--- |
| **Connector** | Trino (Native Power BI Connector) or ODBC |
| **Host / Server URL** | `http://localhost:8082` (or `localhost:8082`) |
| **Port** | `8082` |
| **Catalog** | `delta` |
| **Schema** | `movie_lakehouse` |
| **Authentication** | None / Anonymous (or Username: `admin`) |
| **Data Connectivity Mode** | **DirectQuery** (for real-time streaming updates) or **Import** |

---

## 2. Power BI SQL Queries (Datasets)

When creating the queries via Power BI's Trino connector or SQL interface:

### Dataset 1: `StagingEvents` (Clean Validated Events)
```sql
SELECT 
    event_id,
    event_type,
    event_date,
    event_hour,
    user_id,
    session_id,
    device,
    country,
    tmdb_id,
    title,
    genres,
    popularity,
    vote_average,
    vote_count,
    view_duration_seconds,
    user_rating,
    rating_bracket,
    engagement_score,
    validation_status
FROM delta.movie_lakehouse.staging_events
```

### Dataset 2: `QuarantineEvents` (Dead-Letter Queue / Schema Violations)
```sql
SELECT 
    event_id,
    event_type,
    event_date,
    rejection_reason,
    tmdb_id,
    title,
    vote_average,
    corrupt_record_raw
FROM delta.movie_lakehouse.quarantine_events
```

*(Note: Pre-exported CSV snapshot datasets are also available at `data/analytics_exports/cat2_staging_events.csv` and `data/analytics_exports/cat2_quarantine_events.csv` for immediate offline import).*

---

## 3. Power BI DAX Measures

In Power BI Desktop, navigate to **Modeling > New Measure** and create the following analytical measures:

### 1. Total Ingested Events
```dax
Total Ingested Events = 
COUNTROWS('StagingEvents') + COUNTROWS('QuarantineEvents')
```

### 2. Valid Events Count
```dax
Valid Events Count = 
COUNTROWS('StagingEvents')
```

### 3. Quarantined Events Count (DLQ)
```dax
Quarantined Events Count = 
COUNTROWS('QuarantineEvents')
```

### 4. Validation Success Rate %
```dax
Validation Success Rate % = 
DIVIDE([Valid Events Count], [Total Ingested Events], 0)
```
*(Format as Percentage `0.0%`)*

### 5. Quarantine Failure Rate %
```dax
Quarantine Failure Rate % = 
DIVIDE([Quarantined Events Count], [Total Ingested Events], 0)
```
*(Format as Percentage `0.0%`)*

### 6. Average Engagement Score
```dax
Average Engagement Score = 
AVERAGE('StagingEvents'[engagement_score])
```
*(Format as Decimal `0.00`)*

### 7. Average TMDB Vote
```dax
Average TMDB Vote = 
AVERAGE('StagingEvents'[vote_average])
```
*(Format as Decimal `0.00`)*

### 8. Masterpiece Share %
```dax
Masterpiece Share % = 
DIVIDE(
    CALCULATE(COUNTROWS('StagingEvents'), 'StagingEvents'[rating_bracket] = "Masterpiece"),
    COUNTROWS('StagingEvents'),
    0
)
```

---

## 4. Visual Layout & Dashboard Specification

Construct the following 5 visual components on the report canvas:

| # | Visual Type | Title | Data Fields / Configuration | Purpose |
|---|---|---|---|---|
| **1** | **Card Matrix / Multi-row Card** | *Lakehouse Health KPIs* | Measures: `[Total Ingested Events]`, `[Valid Events Count]`, `[Quarantined Events Count]`, `[Validation Success Rate %]` | Executive overview of streaming volume and data quality. |
| **2** | **Clustered Bar Chart** | *Quarantine DLQ Root-Cause Breakdown* | **Y-Axis**: `'QuarantineEvents'[rejection_reason]`<br>**X-Axis**: `Count of event_id` | Identifies schema enforcement violations (`CORRUPT_JSON_SYNTAX`, `OUT_OF_BOUNDS`, `MISSING_TMDB_ID`). |
| **3** | **Donut Chart** | *Rating Bracket Distribution* | **Legend**: `'StagingEvents'[rating_bracket]`<br>**Values**: `Count of event_id` | Displays volume segmentation across curated rating brackets. |
| **4** | **Clustered Column Chart** | *Movie Title Engagement Ranking* | **X-Axis**: `'StagingEvents'[title]`<br>**Y-Axis**: `[Average Engagement Score]`, `Average of vote_average` | Ranks top movies by real-time streaming engagement metrics. |
| **5** | **Table Visual & Slicer** | *Recent Stream Records & Date Slicer* | **Slicer**: `'StagingEvents'[event_date]`<br>**Columns**: `event_id`, `title`, `rating_bracket`, `vote_average`, `engagement_score` | Detailed operational drill-down by date partitions. |

---

## 5. Saving as `Cat-2 Lakehouse Dashboard.pbix`

1. Open **Power BI Desktop**.
2. Click **Get Data > Other > Trino** (or **Text/CSV** pointing to `data/analytics_exports/`).
3. Enter Server: `localhost:8082`, Catalog: `delta`, Schema: `movie_lakehouse`.
4. Create the 8 DAX measures above.
5. Drag and drop the 5 visuals as defined.
6. Click **File > Save As** and save as:
   `Cat-2 Lakehouse Dashboard.pbix` in the workspace root directory.
