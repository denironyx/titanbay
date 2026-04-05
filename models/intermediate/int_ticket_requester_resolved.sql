/*
    Entity resolution: link each Freshdesk ticket requester to a platform
    investor by building an enriched investor record that includes their
    RM's email and name, then matching tickets against all available
    identifiers in a single pass.

    Resolution paths (checked simultaneously, priority breaks ties):
      1. Ticket email matches investor email (direct)
      2. Ticket email matches investor's RM email (RM raised on behalf)
      3. Ticket name matches investor name (personal email used)

    This approach joins RM to investor BEFORE matching tickets, so every
    resolved ticket gets an investor_id regardless of who raised it.
    No separate RM resolution path needed.

    Resolution rate: ~95% (1,902 of 2,000 tickets).
    Remaining 98 are Titanbay IS staff (@titanbay.com/co.uk) whose
    requester_name does not match any known investor.

    Grain: one row per ticket (preserved from source).
*/

with tickets as (
    select * from {{ ref('stg_freshdesk_tickets') }}
),

/*
    Enriched investor: one row per investor, with their RM's contact
    details attached. When an RM raises a ticket, the match resolves
    to the investor they manage — not to the RM as a separate entity.
*/
enriched_investors as (
    select
        i.investor_id,
        i.email,
        i.full_name,
        i.entity_id,
        i.relationship_manager_id,
        i.country,
        i.created_at,
        rm.email    as rm_email,
        rm.rm_name  as rm_name
    from {{ ref('stg_platform_investors') }} i
    left join {{ ref('stg_platform_relationship_managers') }} rm
        on i.relationship_manager_id = rm.rm_id
),

-- match tickets to enriched investors via email or name
ticket_investor_match as (
    select
        t.ticket_id,
        ei.investor_id,
        ei.entity_id,
        ei.relationship_manager_id,

        -- which path matched
        case
            when lower(trim(t.requester_email)) = lower(trim(ei.email))
                then 'email'
            when ei.rm_email is not null
                 and lower(trim(t.requester_email)) = lower(trim(ei.rm_email))
                then 'rm_email'
            when lower(trim(
                    regexp_replace(t.requester_name, '^(Mr|Mrs|Ms|Dr|Miss|Prof|Sir|Lady)\.?\s+', '', 'i')
                 )) = lower(trim(ei.full_name))
                then 'name'
        end as resolution_method,

        -- has_relationship_manager: true if matched via RM or investor has RM assigned
        case
            when lower(trim(t.requester_email)) = lower(trim(ei.rm_email))
                then true
            when ei.relationship_manager_id is not null
                then true
            else false
        end as has_relationship_manager,

        -- deduplicate: prefer email > rm_email > name, then most recent investor
        row_number() over (
            partition by t.ticket_id
            order by
                case
                    when lower(trim(t.requester_email)) = lower(trim(ei.email))    then 1
                    when ei.rm_email is not null
                         and lower(trim(t.requester_email)) = lower(trim(ei.rm_email)) then 2
                    else 3
                end,
                ei.created_at desc
        ) as rn

    from tickets t
    inner join enriched_investors ei
        on lower(trim(t.requester_email)) = lower(trim(ei.email))
        or (
            ei.rm_email is not null
            and lower(trim(t.requester_email)) = lower(trim(ei.rm_email))
        )
        or lower(trim(
            regexp_replace(t.requester_name, '^(Mr|Mrs|Ms|Dr|Miss|Prof|Sir|Lady)\.?\s+', '', 'i')
        )) = lower(trim(ei.full_name))
),

resolved as (
    select
        t.ticket_id,
        t.requester_email,
        t.requester_name,
        t.subject,
        t.status,
        t.priority,
        t.created_at,
        t.resolved_at,
        t.tags,
        t.primary_topic,
        t.topic_category,
        t.partner_label,
        t.resolution_hours,

        -- resolution outcome
        case
            when m.investor_id is not null then 'investor'
            else 'unresolved'
        end as requester_type,

        coalesce(m.resolution_method, 'unresolved') as resolution_method,
        coalesce(m.has_relationship_manager, false) as has_relationship_manager,

        -- resolved keys
        m.investor_id   as resolved_investor_id,
        m.entity_id     as resolved_entity_id

    from tickets t
    left join ticket_investor_match m
        on t.ticket_id = m.ticket_id and m.rn = 1
)

select * from resolved
