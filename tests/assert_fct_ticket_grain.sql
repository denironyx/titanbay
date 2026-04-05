/*
    Grain test: fct_support_tickets must have exactly the same number
    of rows as the staging source. Any delta indicates grain explosion
    or lost rows during entity resolution.
*/

with fact_count as (
    select count(*) as cnt from {{ ref('fct_support_tickets') }}
),

source_count as (
    select count(*) as cnt from {{ ref('stg_freshdesk_tickets') }}
)

select
    f.cnt as fact_rows,
    s.cnt as source_rows,
    f.cnt - s.cnt as delta
from fact_count f
cross join source_count s
where f.cnt != s.cnt
