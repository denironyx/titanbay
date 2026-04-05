# Runbook — Operating and Evolving the Platform

This document covers how to run the project, adapt it for BigQuery, and the forward-looking
design thinking behind where it should go next.


### BigQuery Adapter

To run this project on BigQuery instead of Postgres, two things change: the connection
profile and a handful of Postgres-specific SQL functions.

**Step 1 — Install the adapter and configure the profile:**

```bash
pip install dbt-bigquery
```

Replace `profiles.yml` with:

```yaml
titanbay:
  outputs:
    dev:
      type: bigquery
      method: oauth            # or service-account
      project: your-gcp-project
      dataset: titanbay        # BigQuery dataset name
      threads: 4
      location: EU             # or US, depending on data residency
      # For service account auth:
      # keyfile: /path/to/keyfile.json
  target: dev
```

**Step 2 — SQL changes required:**

The project uses several Postgres-specific constructs. Below is every change needed,
by file:

#### `stg_freshdesk_tickets.sql`

| Line | Postgres | BigQuery |
|------|----------|----------|
| 20 | `extract(epoch from (resolved_at - created_at)) / 3600.0` | `timestamp_diff(resolved_at, created_at, second) / 3600.0` |

#### `stg_seed_countries.sql`

| Line | Postgres | BigQuery |
|------|----------|----------|
| 18-24 | `split_part(str, ':', 1)` / `split_part(str, ':', 2)` | `cast(split(str, ':')[offset(0)] as float64)` / `cast(split(str, ':')[offset(1)] as float64)` |
| 18-24 | `cast(... as decimal)` | `cast(... as float64)` |

#### `int_country_resolved.sql`

| Line | Postgres | BigQuery |
|------|----------|----------|
| 133 | `null::varchar(3) as place_id` | `cast(null as string) as place_id` |

#### `fct_support_tickets.sql`

| Line | Postgres | BigQuery |
|------|----------|----------|
| 114 | `rt.created_at::date` | `date(rt.created_at)` |
| 115 | `rt.resolved_at::date` | `date(rt.resolved_at)` |
| 103 | `split_part(trim(rt.partner_label), ' ', 1)` | `split(trim(rt.partner_label), ' ')[offset(0)]` |
| 103 | `\|\|` (string concat) | `concat(...)` |
| 182 | `'{{ var("run_date") }}'::date` | `parse_date('%Y-%m-%d', '{{ var("run_date") }}')` |

#### `obt_ticket_analysis.sql`

| Line | Postgres | BigQuery |
|------|----------|----------|
| 74 | `t.created_at::date` | `date(t.created_at)` |
| 81 | `abs(fc.scheduled_close_date - t.created_at::date)` | `abs(date_diff(fc.scheduled_close_date, date(t.created_at), day))` |
| 153 | `make_interval(hours => geo.timezone_offset_hours::int)` | `interval cast(geo.timezone_offset_hours as int64) hour` |
| 153 | `extract(hour from timestamp + interval)` | `extract(hour from timestamp_add(t.created_at, interval cast(geo.timezone_offset_hours as int64) hour))` |
| 164 | `to_char(t.created_at, 'Day')` | `format_date('%A', date(t.created_at))` |
| 166 | `to_char(t.created_at, 'Month')` | `format_date('%B', date(t.created_at))` |
| 169 | `extract(dow from t.created_at) in (0, 6)` | `extract(dayofweek from t.created_at) in (1, 7)` (BigQuery: 1=Sun, 7=Sat) |

#### `tests/assert_entity_resolution_coverage.sql`

| Line | Postgres | BigQuery |
|------|----------|----------|
| 10-12 | `count(*) filter (where ...)` | `countif(...)` |

**Step 3 — Incremental strategy:**

BigQuery supports `delete+insert` natively via the `dbt-bigquery` adapter. The
`incremental_strategy='delete+insert'` config works without changes. For large datasets,
consider switching to `incremental_strategy='merge'` with a `partition_by` config on
`created_date_key` for BigQuery-native partition pruning:

```sql
{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    incremental_strategy='merge',
    partition_by={'field': 'created_date_key', 'data_type': 'date'}
) }}
```

**Step 4 — Packages:**

All three packages (`dbt_utils`, `dbt_expectations`) support BigQuery.
No changes to `packages.yml` needed.
