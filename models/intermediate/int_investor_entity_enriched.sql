/*
    Enriches investors with entity and partner context.
    Grain: one row per investor (not per investor-entity, because
    investors.entity_id is a direct FK, not a bridge table).
*/

with investors as (
    select * from {{ ref('stg_platform_investors') }}
),

entities as (
    select * from {{ ref('stg_platform_entities') }}
),

partners as (
    select * from {{ ref('stg_platform_partners') }}
),

enriched as (
    select
        i.investor_id,
        i.user_id,
        i.email,
        i.full_name,
        i.country,
        i.created_at,
        i.relationship_manager_id,

        -- entity context
        e.entity_id,
        e.entity_name,
        e.entity_type,
        e.kyc_status,

        -- partner context (via entity)
        e.partner_id,
        p.partner_name,
        p.partner_type

    from investors i
    left join entities e
        on i.entity_id = e.entity_id
    left join partners p
        on e.partner_id = p.partner_id
)

select * from enriched
