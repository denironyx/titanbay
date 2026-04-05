/*
    Grain test: obt_ticket_analysis must match the RESOLVED subset of
    fct_support_tickets. The OBT intentionally excludes unresolved tickets
    (Titanbay IS staff with no investor match).

    Any difference indicates a fan-out from joins or a filter mismatch.
*/

with obt_count as (
    select count(*) as cnt from {{ ref('obt_ticket_analysis') }}
),

fact_resolved_count as (
    select count(*) as cnt
    from {{ ref('fct_support_tickets') }}
    where requester_type != 'unresolved'
)

select
    o.cnt as obt_rows,
    f.cnt as fact_resolved_rows,
    o.cnt - f.cnt as delta
from obt_count o
cross join fact_resolved_count f
where o.cnt != f.cnt
