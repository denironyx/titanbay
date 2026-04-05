/*
    Fact table: one row per support ticket with resolved investor context.

    Joins resolved tickets to the investor-entity-partner chain using
    natural keys (UUIDs) — no surrogate keys needed because the source
    data is already cleanly normalised with stable identifiers.

    Partner attribution:
      1. Investor → entity → partner (structural FK, covers 95% of tickets)
      2. Fuzzy partner_label match (fallback for the 5% unresolved tickets)

    Materialization: incremental (delete+insert by ticket_id).
    Date-partitioned via var('run_date').
*/

{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        incremental_strategy='delete+insert',
        tags=['fact']
    )
}}

with resolved_tickets as (
    select * from {{ ref('int_ticket_requester_resolved') }}
),

enriched_investors as (
    select * from {{ ref('int_investor_entity_enriched') }}
),

partners as (
    select * from {{ ref('stg_platform_partners') }}
),

-- fuzzy partner_label match (fallback for unresolved tickets)
label_partner_ranked as (
    select
        rt.ticket_id,
        p.partner_id,
        p.partner_name,
        row_number() over (
            partition by rt.ticket_id
            order by length(p.partner_name)
        ) as rn
    from resolved_tickets rt
    cross join partners p
    where rt.partner_label is not null
      and (
          lower(trim(rt.partner_label)) = lower(p.partner_name)
          or lower(trim(rt.partner_label)) like '%' || lower(p.partner_name) || '%'
          or lower(p.partner_name) like '%' || lower(trim(rt.partner_label)) || '%'
          or (
              length(trim(rt.partner_label)) >= 3
              and lower(p.partner_name) like lower(split_part(trim(rt.partner_label), ' ', 1)) || '%'
          )
      )
)

select
    rt.ticket_id,
    rt.created_at::date                                     as created_date_key,

    -- investor context (natural keys)
    rt.resolved_investor_id                                 as investor_id,
    ei.entity_id,
    coalesce(ei.partner_id, lpr.partner_id)                 as partner_id,

    -- entity resolution
    rt.requester_type,
    rt.resolution_method,
    rt.has_relationship_manager,
    rt.requester_email,
    rt.requester_name,

    -- investor attributes (from enrichment)
    ei.full_name                                            as investor_name,
    ei.country,
    ei.entity_name,
    ei.entity_type,
    ei.kyc_status,
    coalesce(ei.partner_name, lpr.partner_name)             as partner_name,
    ei.partner_type,

    -- ticket attributes
    rt.subject,
    rt.status,
    rt.priority,
    rt.created_at,
    rt.resolved_at,
    rt.tags,
    rt.primary_topic,
    rt.topic_category,
    rt.partner_label,

    -- measures
    rt.resolution_hours

from resolved_tickets rt

left join enriched_investors ei
    on rt.resolved_investor_id = ei.investor_id

-- partner via label (fuzzy, fallback only when investor chain has no partner)
left join label_partner_ranked lpr
    on rt.ticket_id = lpr.ticket_id
    and lpr.rn = 1
    and ei.partner_id is null

{% if is_incremental() and var('run_date', none) is not none %}
where rt.created_at::date = '{{ var("run_date") }}'::date
{% endif %}
