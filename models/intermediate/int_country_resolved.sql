/*
    Resolves free-text country names from platform_investors to
    standardised ISO 3166-1 alpha-3 codes.

    Three-step waterfall:
      1. Exact match on common_name (handles ~90% of countries)
      2. Exact match on official_name (handles formal names like "Kyrgyz Republic")
      3. CASE WHEN fallback — aliases, accents, parentheticals, legacy names,
         and ambiguous short forms. All inline, no seed dependency.

    Grain: one row per distinct source country name.
*/

with investor_countries as (
    select distinct
        country as source_country_name
    from {{ ref('stg_platform_investors') }}
    where country is not null and trim(country) != ''
),

countries as (
    select * from {{ ref('stg_seed_countries') }}
),

-- step 1: exact match on common_name
exact_common as (
    select
        ic.source_country_name,
        c.cca3 as place_id,
        'common_name' as match_method
    from investor_countries ic
    inner join countries c
        on lower(trim(ic.source_country_name)) = lower(trim(c.common_name))
),

-- step 2: exact match on official_name
remaining_after_common as (
    select ic.source_country_name
    from investor_countries ic
    left join exact_common ec
        on ic.source_country_name = ec.source_country_name
    where ec.source_country_name is null
),

exact_official as (
    select
        r.source_country_name,
        c.cca3 as place_id,
        'official_name' as match_method
    from remaining_after_common r
    inner join countries c
        on lower(trim(r.source_country_name)) = lower(trim(c.official_name))
),

/*
    Step 3: CASE WHEN fallback for everything the reference data
    doesn't cover. Organised by category:
      - Formal/inverted names (from old ISO conventions)
      - Legacy regime names
      - Accent-stripped variants
      - Parenthetical suffixes
      - Ambiguous short forms (documented decisions)
*/
remaining_after_official as (
    select r.source_country_name
    from remaining_after_common r
    left join exact_official eo
        on r.source_country_name = eo.source_country_name
    where eo.source_country_name is null
),

fallback_match as (
    select
        r.source_country_name,
        case
            -- formal / inverted name conventions
            when lower(trim(r.source_country_name)) like 'korea (republic%'                   then 'KOR'
            when lower(trim(r.source_country_name)) like 'korea (democratic%'                  then 'PRK'
            when lower(trim(r.source_country_name)) like 'congo (democratic%'                  then 'COD'
            when lower(trim(r.source_country_name)) like 'congo (republic%'                    then 'COG'
            when lower(trim(r.source_country_name)) like 'palestine%'                          then 'PSE'
            when lower(trim(r.source_country_name)) like 'micronesia%'                         then 'FSM'
            when lower(trim(r.source_country_name)) like 'moldova%'                            then 'MDA'
            when lower(trim(r.source_country_name)) like 'tanzania%'                           then 'TZA'
            when lower(trim(r.source_country_name)) like 'venezuela%'                          then 'VEN'
            when lower(trim(r.source_country_name)) like 'iran%'                               then 'IRN'
            when lower(trim(r.source_country_name)) like 'lao%'                                then 'LAO'
            when lower(trim(r.source_country_name)) like 'brunei%'                             then 'BRN'
            when lower(trim(r.source_country_name)) like 'syrian%'                             then 'SYR'

            -- legacy / alternate names
            when lower(trim(r.source_country_name)) like 'french southern%'                    then 'ATF'
            when lower(trim(r.source_country_name)) like 'holy see%'                           then 'VAT'
            when lower(trim(r.source_country_name)) = 'kyrgyz republic'                        then 'KGZ'
            when lower(trim(r.source_country_name)) = 'cabo verde'                             then 'CPV'
            when lower(trim(r.source_country_name)) = 'czech republic'                         then 'CZE'
            when lower(trim(r.source_country_name)) = 'swaziland'                              then 'SWZ'
            when lower(trim(r.source_country_name)) = 'burma'                                  then 'MMR'
            when lower(trim(r.source_country_name)) like 'republic of the congo%'              then 'COG'
            when lower(trim(r.source_country_name)) like 'ivory coast%'                        then 'CIV'
            when lower(trim(r.source_country_name)) like 'cote d%'                             then 'CIV'
            when lower(trim(r.source_country_name)) = 'viet nam'                               then 'VNM'
            when lower(trim(r.source_country_name)) like 'libyan%'                             then 'LBY'
            when lower(trim(r.source_country_name)) like 'netherlands antilles%'               then 'NLD'

            -- accent-stripped variants
            when lower(trim(r.source_country_name)) like 'sao tome%'                           then 'STP'
            when lower(trim(r.source_country_name)) like 'reunion%'                            then 'REU'
            when lower(trim(r.source_country_name)) like 'saint barth%'                        then 'BLM'
            when lower(trim(r.source_country_name)) like '%macao%'                             then 'MAC'

            -- parenthetical suffixes
            when lower(trim(r.source_country_name)) like 'slovakia%'                           then 'SVK'
            when lower(trim(r.source_country_name)) like 'falkland%'                           then 'FLK'
            when lower(trim(r.source_country_name)) like 'bouvet island%'                      then 'BVT'
            when lower(trim(r.source_country_name)) like 'british indian ocean%'               then 'IOT'
            when lower(trim(r.source_country_name)) like 'antarctica%'                         then 'ATA'
            when lower(trim(r.source_country_name)) like 'saint helena%'                       then 'SHN'
            when lower(trim(r.source_country_name)) like 'svalbard%'                           then 'SJM'

            -- ambiguous short forms (documented decisions)
            when lower(trim(r.source_country_name)) = 'korea'                                  then 'KOR'  -- South Korea; North Korea is sanctioned
            when lower(trim(r.source_country_name)) = 'congo'                                  then 'COG'  -- Republic of Congo; DRC typically specified explicitly
        end as place_id,
        'fallback' as match_method
    from remaining_after_official r
),

-- flag anything still unresolved
remaining_unresolved as (
    select r.source_country_name
    from remaining_after_official r
    left join fallback_match fm
        on r.source_country_name = fm.source_country_name
        and fm.place_id is not null
    where fm.source_country_name is null
),

unresolved as (
    select
        source_country_name,
        null::varchar(3) as place_id,
        'unresolved' as match_method
    from remaining_unresolved
),

combined as (
    select * from exact_common
    union all
    select * from exact_official
    union all
    select source_country_name, place_id, match_method from fallback_match where place_id is not null
    union all
    select * from unresolved
)

select * from combined
