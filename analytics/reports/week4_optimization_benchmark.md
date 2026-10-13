# Week 4: Trino Query Performance & Lakehouse Optimization Benchmark Report

- **Execution Timestamp**: 2026-10-13T19:45:05.543091+00:00 UTC
- **Query Engine**: Trino 435 (Docker `de-trino` on port 8082)
- **Storage Format**: Delta Lake 4.4.1 (ACID Parquet + Write-Ahead Log)
- **Catalog**: Trino `delta_lake` connector (`delta.movie_lakehouse`)

## 1. Storage & Compaction Metrics

| Table Name | Initial Files | Compacted Files | Initial Size (KB) | Post-Compaction Size (KB) | Compaction Time (s) | Vacuum Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `movie_lakehouse.staging_events` | 2 | 2 | 19.35 | 19.35 | 0.069 | 168h Safe Retain |
| `movie_lakehouse.quarantine_events` | 2 | 2 | 18.67 | 18.67 | 0.012 | 168h Safe Retain |

## 2. Trino Analytical Query Latency Comparison

| Query ID | Query Name & Workload | Latency Before (ms) | Latency After (ms) | Speedup / Improvement |
| :--- | :--- | :---: | :---: | :---: |
| `Q1_TOTAL_SUMMARY` | Volume & Validation Rate Summary | 313.57 ms | 236.78 ms | **+24.5% faster (+76.8 ms)** |
| `Q2_ENGAGEMENT_AGG` | Top Movies by Engagement & Ratings | 337.03 ms | 256.51 ms | **+23.9% faster (+80.5 ms)** |
| `Q3_RATING_BRACKETS` | Rating Bracket Metric Distribution | 256.50 ms | 259.90 ms | **-1.3% (-3.4 ms)** |
| `Q4_PARTITION_FILTER` | Date Partition Pruned Event Scan | 285.53 ms | 284.85 ms | **+0.2% faster (+0.7 ms)** |
| `Q5_QUARANTINE_REASONS` | Quarantine DLQ Root-Cause Breakdown | 328.50 ms | 287.67 ms | **+12.4% faster (+40.8 ms)** |

## 3. Query Definitions

### `Q1_TOTAL_SUMMARY`: Volume & Validation Rate Summary
*Calculates overall counts and ratios across staging and DLQ tables.*
```sql
SELECT 
            (SELECT count(*) FROM delta.movie_lakehouse.staging_events) AS valid_events,
            (SELECT count(*) FROM delta.movie_lakehouse.quarantine_events) AS quarantined_events,
            round(
                CAST((SELECT count(*) FROM delta.movie_lakehouse.staging_events) AS DOUBLE) /
                NULLIF((SELECT count(*) FROM delta.movie_lakehouse.staging_events) + (SELECT count(*) FROM delta.movie_lakehouse.quarantine_events), 0) * 100.0,
                2
            ) AS validation_rate_pct
```

### `Q2_ENGAGEMENT_AGG`: Top Movies by Engagement & Ratings
*Aggregates view events, average engagement score, and vote ratings per title.*
```sql
SELECT 
            title,
            count(*) AS view_count,
            round(avg(vote_average), 2) AS avg_vote,
            round(avg(engagement_score), 2) AS avg_engagement,
            max(engagement_score) AS max_engagement
        FROM delta.movie_lakehouse.staging_events
        GROUP BY title
        ORDER BY avg_engagement DESC, view_count DESC
```

### `Q3_RATING_BRACKETS`: Rating Bracket Metric Distribution
*Groups by curated rating brackets and computes aggregations.*
```sql
SELECT 
            rating_bracket,
            count(*) AS total_records,
            round(avg(vote_average), 2) AS avg_vote,
            round(min(vote_average), 2) AS min_vote,
            round(max(vote_average), 2) AS max_vote,
            round(avg(engagement_score), 2) AS avg_score
        FROM delta.movie_lakehouse.staging_events
        GROUP BY rating_bracket
        ORDER BY total_records DESC
```

### `Q4_PARTITION_FILTER`: Date Partition Pruned Event Scan
*Scans partitioned files matching recent event_date and filters by threshold.*
```sql
SELECT 
            event_date,
            count(*) AS daily_views,
            round(avg(engagement_score), 2) AS daily_avg_engagement
        FROM delta.movie_lakehouse.staging_events
        WHERE event_date >= CAST('2026-10-13' AS DATE)
        GROUP BY event_date
        ORDER BY event_date DESC
```

### `Q5_QUARANTINE_REASONS`: Quarantine DLQ Root-Cause Breakdown
*Aggregates schema and data validation failure causes from quarantine table.*
```sql
SELECT 
            rejection_reason,
            count(*) AS failure_count,
            round(CAST(count(*) AS DOUBLE) / (SELECT count(*) FROM delta.movie_lakehouse.quarantine_events) * 100.0, 2) AS pct_of_total
        FROM delta.movie_lakehouse.quarantine_events
        GROUP BY rejection_reason
        ORDER BY failure_count DESC
```
