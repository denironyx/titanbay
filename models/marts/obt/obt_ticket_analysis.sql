/*
    One Big Table: fully denormalized analyst-facing table.
    Grain: one row per support ticket.

    Zero-join self-service analytics — every dimension pre-joined.
    Designed to answer:

    1. Which investors raise the most tickets, and what patterns exist?
       → investor_lifetime_ticket_count, is_repeat_investor, ticket_rank
       → Sliceable by partner, entity_type, geography, topic, priority

    2. When will the IS team be under pressure?
       → nearest_fund_close_name / days_to_nearest_close (partner-scoped)
       → Close dates are known in advance — historical patterns forecast load

    Materialization: incremental (delete+insert by ticket_id).
    Date-partitioned via var('run_date').
*/

{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        incremental_strategy='delete+insert',
        tags=['obt']
    )
}}

with tickets as (
    select * from {{ ref('fct_support_tickets') }}
),

countries as (
    select * from {{ ref('stg_seed_countries') }}
),

country_resolved as (
    select * from {{ ref('int_country_resolved') }}
),

fund_closes as (
    select * from {{ ref('int_fund_close_commitments') }}
),

/*
    Close-proximity correlation: link each ticket to the nearest fund close
    belonging to the SAME partner, within +/- 30 days of ticket creation.
    Partner-scoped so an Ashford investor's ticket isn't correlated with
    a Hadley fund close.
*/
nearest_close as (
    select
        t.ticket_id,
        fc.fund_name,
        fc.scheduled_close_date,
        fc.close_status,
        fc.total_committed_aum,
        fc.close_number,
        (fc.scheduled_close_date - t.created_date_key) as days_to_close,
        row_number() over (
            partition by t.ticket_id
            order by abs(fc.scheduled_close_date - t.created_date_key)
        ) as rn
    from tickets t
    inner join fund_closes fc
        on t.partner_id = fc.partner_id
    where t.partner_id is not null
      and abs(fc.scheduled_close_date - t.created_date_key) <= 30
),

-- investor-level ticket stats
investor_ticket_stats as (
    select
        investor_id,
        count(*) as lifetime_ticket_count
    from tickets
    where investor_id is not null
    group by 1
),

obt as (
    select
        -- ticket identifiers
        t.ticket_id,

        -- entity resolution
        t.requester_email,
        t.requester_name,

        -- ticket attributes
        t.subject,
        t.status,
        t.priority,
        t.tags,
        t.primary_topic,
        t.topic_category,
        t.partner_label,

        -- timestamps
        t.created_at,
        t.resolved_at,

        -- measures
        t.resolution_hours,

        -- investor context
        t.investor_id,
        t.investor_name,
        t.country                   as investor_country_raw,

        -- entity context
        t.entity_id,
        t.entity_name,
        t.entity_type,
        t.kyc_status,

        -- partner context
        t.partner_id,
        t.partner_name,
        t.partner_type,

        
        t.requester_type,
        t.resolution_method,
        t.has_relationship_manager,

        -- geography context (from country resolution + seed)
        cr.place_id,
        c.cca2                      as country_code,
        c.common_name               as investor_country,
        c.region                    as investor_region,
        c.subregion                 as investor_subregion,
        c.primary_timezone          as investor_timezone,
        c.timezone_offset_hours     as investor_timezone_offset_hours,

        -- local hour approximation
        case
            when c.timezone_offset_hours is not null
            then extract(hour from t.created_at + make_interval(hours => c.timezone_offset_hours::int))
        end                         as ticket_created_local_hour,

        -- nearest fund close context (partner-scoped)
        nc.fund_name                as nearest_fund_close_name,
        nc.scheduled_close_date     as nearest_fund_close_date,
        nc.days_to_close            as days_to_nearest_close,
        nc.total_committed_aum      as nearest_close_committed_aum,
        nc.close_status             as nearest_close_status,

        -- date attributes (inline)
        to_char(t.created_at, 'Day')            as created_day_of_week,
        extract(month from t.created_at)        as created_month,
        to_char(t.created_at, 'Month')          as created_month_name,
        extract(quarter from t.created_at)      as created_quarter,
        extract(year from t.created_at)         as created_year,
        extract(dow from t.created_at) in (0, 6) as created_on_weekend,

        -- repeat investor metrics
        coalesce(its.lifetime_ticket_count, 0)  as investor_lifetime_ticket_count,
        coalesce(its.lifetime_ticket_count, 0) > 1 as is_repeat_investor,

        -- ticket rank for this investor (chronological)
        case
            when t.investor_id is not null
            then row_number() over (
                partition by t.investor_id
                order by t.created_at
            )
        end                         as ticket_rank_for_investor

    from tickets t

    -- geography: investor country → ISO code → country reference
    left join country_resolved cr
        on t.country = cr.source_country_name

    left join countries c
        on cr.place_id = c.cca3

    -- nearest fund close (partner-scoped, closest within 30 days)
    left join nearest_close nc
        on t.ticket_id = nc.ticket_id and nc.rn = 1

    -- repeat investor stats
    left join investor_ticket_stats its
        on t.investor_id = its.investor_id
)

/*
    Filter to resolved tickets only. The 98 unresolved tickets (~5%) are
    Titanbay IS staff (@titanbay.com/co.uk) creating tickets internally —
    they have no investor_id, entity_id, or partner context, so they add
    noise to every analyst GROUP BY without contributing to investor
    behaviour or partner performance analysis.

    These tickets are preserved in fct_support_tickets (the system of
    record) with requester_type = 'unresolved'. The fact table grain test
    ensures no rows are silently lost. The OBT is a curated analytical
    view, not a source of truth.
*/
select * from obt
where investor_id is not null

{% if is_incremental() and var('run_date', none) is not none %}
  and created_at::date = '{{ var("run_date") }}'::date
{% endif %}
