/*
    Data quality test: entity resolution should resolve at least 90% of tickets.
    Current baseline is ~95% (1,902 of 2,000).
    A drop below 90% indicates a data quality problem upstream — new email
    domains, investor name format changes, or RM assignment gaps.
*/

with stats as (
    select
        count(*) as total_tickets,
        count(*) filter (where requester_type != 'unresolved') as resolved_tickets,
        round(
            count(*) filter (where requester_type != 'unresolved') * 100.0 / count(*),
            1
        ) as resolution_pct
    from {{ ref('fct_support_tickets') }}
)

select *
from stats
where resolution_pct < 90
