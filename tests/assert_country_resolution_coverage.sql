/*
    Country resolution should cover all investor countries.
    Fails if any country name could not be resolved.
    Fix: add a CASE WHEN entry in int_country_resolved.sql fallback_match CTE.
*/

select
    source_country_name,
    match_method
from {{ ref('int_country_resolved') }}
where match_method = 'unresolved'
