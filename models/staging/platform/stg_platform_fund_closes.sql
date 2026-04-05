with source as (
    select * from {{ source('platform', 'platform_fund_closes') }}
),

cleaned as (
    select
        close_id,
        fund_id,
        trim(fund_name)                 as fund_name,
        partner_id,
        close_number,
        scheduled_close_date,
        lower(trim(close_status))       as close_status,
        total_committed_aum
    from source
)

select * from cleaned
