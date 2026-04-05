with source as (
    select * from {{ source('country', 'seed_countries') }}
),

cleaned as (
    select
        trim(common_name)       as common_name,
        trim(official_name)     as official_name,
        upper(trim(cca2))       as cca2,
        upper(trim(cca3))       as cca3,
        trim(region)            as region,
        trim(subregion)         as subregion,
        trim(primary_timezone)  as primary_timezone,
        -- parse timezone offset to numeric hours
        case
            when trim(primary_timezone) = 'UTC' then 0.0
            when trim(primary_timezone) like 'UTC+%:%' then
                cast(split_part(replace(trim(primary_timezone), 'UTC+', ''), ':', 1) as decimal)
                + cast(split_part(replace(trim(primary_timezone), 'UTC+', ''), ':', 2) as decimal) / 60.0
            when trim(primary_timezone) like 'UTC-%:%' then
                -1.0 * (
                    cast(split_part(replace(trim(primary_timezone), 'UTC-', ''), ':', 1) as decimal)
                    + cast(split_part(replace(trim(primary_timezone), 'UTC-', ''), ':', 2) as decimal) / 60.0
                )
            else null
        end as timezone_offset_hours
    from source
)

select * from cleaned
