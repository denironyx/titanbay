/*
    Sanity check: no ticket should have a created_at date in the future
    (relative to the latest date in the dataset, allowing for synthetic data).
*/

select
    ticket_id,
    created_at,
    resolved_at
from {{ ref('stg_freshdesk_tickets') }}
where resolved_at is not null
  and resolved_at < created_at
