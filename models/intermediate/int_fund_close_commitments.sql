/*
    Fund close data with partner context.
    Grain: one row per fund close.
*/

with fund_closes as (
    select * from {{ ref('stg_platform_fund_closes') }}
),

partners as (
    select * from {{ ref('stg_platform_partners') }}
),

enriched as (
    select
        fc.close_id,
        fc.fund_id,
        fc.fund_name,
        fc.partner_id,
        p.partner_name,
        fc.close_number,
        fc.scheduled_close_date,
        fc.close_status,
        fc.total_committed_aum
    from fund_closes fc
    left join partners p
        on fc.partner_id = p.partner_id
)

select * from enriched
