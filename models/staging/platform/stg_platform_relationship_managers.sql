with source as (
    select * from {{ source('platform', 'platform_relationship_managers') }}
),

cleaned as (
    select
        rm_id,
        partner_id,
        trim(name)          as rm_name,
        lower(trim(email))  as email
    from source
)

select * from cleaned
