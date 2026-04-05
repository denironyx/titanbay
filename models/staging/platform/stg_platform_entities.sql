with source as (
    select * from {{ source('platform', 'platform_entities') }}
),

cleaned as (
    select
        entity_id,
        trim(entity_name)       as entity_name,
        partner_id,
        lower(trim(entity_type)) as entity_type,
        lower(trim(kyc_status))  as kyc_status
    from source
)

select * from cleaned
