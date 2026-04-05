with source as (
    select * from {{ source('platform', 'platform_partners') }}
),

cleaned as (
    select
        partner_id,
        trim(partner_name)       as partner_name,
        lower(trim(partner_type)) as partner_type
    from source
)

select * from cleaned
