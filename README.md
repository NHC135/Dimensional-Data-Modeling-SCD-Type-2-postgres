# Actors Dimension Table — SCD Type 2 Pipeline

This project implements a **Slowly Changing Dimension (SCD) Type 2** pipeline for tracking actor data over time. It captures historical changes in an actor's quality classification and activity status using PostgreSQL.

---

## Project Structure

```
DimHW/
├── actors-table.sql                              # DDL: actors cumulative table + custom types
├── actors-history-scd.sql                        # DDL: actors_history_scd table
├── actors_backfill_generation_query.sql          # Backfill: populate SCD table from full history
├── actors_incremental_cumulative_table_generation_query.sql  # Incremental: update actors table year-over-year
└── actors_incremental_scd_query.sql              # Incremental: update SCD history table year-over-year
```

---

## Data Model

### Custom Types

**`films` (composite type)**
| Field    | Type  | Description              |
|----------|-------|--------------------------|
| film     | TEXT  | Film title               |
| votes    | INT   | Number of votes          |
| rating   | REAL  | Film rating              |
| filmid   | TEXT  | Unique film identifier   |

**`quality_class` (enum)**
| Value    | Rating Range   |
|----------|----------------|
| `star`   | avg_rating > 8 |
| `good`   | avg_rating > 7 |
| `average`| avg_rating > 6 |
| `bad`    | avg_rating ≤ 6 |

---

### `actors` Table (Cumulative)
| Column        | Type           | Description                          |
|---------------|----------------|--------------------------------------|
| actor         | TEXT           | Actor name                           |
| actorid       | TEXT           | Unique actor identifier              |
| films         | films[]        | Array of films for the current year  |
| quality_class | quality_class  | Actor's quality tier                 |
| is_active     | BOOLEAN        | Whether the actor was active that year|
| current_year  | INT            | The year of the record               |

**Primary Key:** `(actorid, current_year)`

---

### `actors_history_scd` Table (SCD Type 2)
| Column        | Type          | Description                                  |
|---------------|---------------|----------------------------------------------|
| actorid       | TEXT          | Unique actor identifier                      |
| actor         | TEXT          | Actor name                                   |
| quality_class | quality_class | Actor's quality tier during this period      |
| is_active     | BOOLEAN       | Activity status during this period           |
| start_year    | INTEGER       | First year of this record's validity         |
| end_year      | INTEGER       | Last year of this record's validity          |
| current_year  | INTEGER       | The processing year (snapshot marker)        |

**Primary Key:** `(actorid, start_year, end_year)`

---

## Pipeline Overview

### Step 1 — DDL Setup
Run the two DDL scripts first to create the necessary types and tables:
```sql
-- 1. Create custom types and actors table
\i actors-table.sql

-- 2. Create SCD history table
\i actors-history-scd.sql
```

---

### Step 2 — Backfill (One-time historical load)
**File:** `actors_backfill_generation_query.sql`

Populates `actors_history_scd` with the full historical record up to a cutoff year (currently `2021`). Uses a three-step CTE pattern:

1. **`with_previous`** — Uses `LAG()` to compare each year's `quality_class` and `is_active` against the prior year
2. **`with_indicators`** — Flags rows where a change occurred (`change_indicator = 1`)
3. **`with_streaks`** — Uses a running `SUM()` of change indicators to group consecutive unchanged periods into streaks

The final `INSERT` collapses each streak into a single SCD row with `MIN(current_year)` as `start_year` and `MAX(current_year)` as `end_year`.

> **Note:** Update the `WHERE current_year <= 2021` filter to match your desired backfill cutoff.

---

### Step 3 — Incremental Cumulative Table Update
**File:** `actors_incremental_cumulative_table_generation_query.sql`

Updates the `actors` cumulative table by merging one new year of data from the source `actor_films` table.

- **`last_year`** — Pulls the previous year's snapshot from `actors`
- **`current_year`** — Aggregates new film data and computes `avg_rating` from `actor_films`
- **`cumulative`** — `FULL OUTER JOIN` merges both, carrying forward historical films and recalculating `quality_class` and `is_active`

> **Note:** Update the year values in `WHERE current_year = 2021` and `WHERE year = 2022` to advance the pipeline by one year.

---

### Step 4 — Incremental SCD Update
**File:** `actors_incremental_scd_query.sql`

Appends one new year of SCD records to `actors_history_scd` by categorizing records into four groups:

| CTE                       | Description                                                   |
|---------------------------|---------------------------------------------------------------|
| `historical_scd`          | Already-closed records (end_year < current processing year)  |
| `unchanged_records`       | Records where quality_class and is_active did not change     |
| `changed_records`         | Records where a change occurred — splits into 2 rows (old + new) using `UNNEST` |
| `new_records`             | Actors appearing for the first time (no prior SCD record)    |

All four sets are combined with `UNION ALL` and stamped with the `current_year`.

> **Note:** Update `WHERE current_year = 2020` and `WHERE current_year = 2021` to advance the pipeline forward.

---

## How to Run (Year-by-Year Incremental)

To advance the pipeline by one year (e.g., from 2021 → 2022):

1. Update year references in `actors_incremental_cumulative_table_generation_query.sql`:
   - `last_year`: `current_year = 2022`
   - `current_year`: `year = 2023`
2. Run the incremental cumulative query to insert the new year into `actors`
3. Update year references in `actors_incremental_scd_query.sql`:
   - `last_year_data`: `current_year = 2021`, `end_year = 2021`
   - `this_year_data`: `current_year = 2022`
   - Final SELECT: `2022 AS current_year`
4. Run the incremental SCD query to update `actors_history_scd`

---

## SCD Type 2 Logic Summary

A new SCD record is created whenever **either** of the following changes between years:
- `quality_class` (e.g., actor moves from `good` → `star`)
- `is_active` (e.g., actor becomes inactive)

When no change is detected, the existing record's `end_year` is extended forward. This preserves full historical trackability of every actor's classification over time.
