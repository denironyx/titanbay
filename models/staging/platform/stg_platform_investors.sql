with source as (
    select * from {{ source('platform', 'platform_investors') }}
),

cleaned as (
    select
        investor_id,
        user_id,
        lower(trim(email))                          as email,
        trim(full_name)                             as full_name,
        entity_id,
        trim(country)                               as country,
        created_at,
        nullif(trim(relationship_manager_id), '')   as relationship_manager_id
    from source
)

select * from cleaned
