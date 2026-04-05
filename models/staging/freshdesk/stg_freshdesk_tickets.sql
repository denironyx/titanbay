with source as (
    select * from {{ source('freshdesk', 'freshdesk_tickets') }}
),

cleaned as (
    select
        ticket_id,
        lower(trim(requester_email))                     as requester_email,
        trim(requester_name)                             as requester_name,
        trim(subject)                                    as subject,
        lower(trim(status))                              as status,
        lower(trim(priority))                            as priority,
        created_at,
        resolved_at,
        trim(tags)                                       as tags,
        nullif(trim(partner_label), '')                  as partner_label,
        -- derived: hours to resolution
        case
            when resolved_at is not null
            then extract(epoch from (resolved_at - created_at)) / 3600.0
        end                                              as resolution_hours,

        /*
            Topic classification: prioritised by business impact.
            Tags are comma-separated (e.g., "kyc,commitment"). The CASE
            picks the highest-priority tag to assign a primary topic.

            Priority order (highest first):
              1. kyc         — can block a close; regulatory requirement
              2. commitment  — directly about capital calls
              3. payment     — money movement
              4. e-signature — close execution
              5. documents   — close-related paperwork
              6. onboarding  — new investor setup
              7. fund-info   — pre-investment research
              8. portal/access/account — platform friction
        */
        case
            when tags like '%kyc%'         then 'kyc'
            when tags like '%commitment%'  then 'commitment'
            when tags like '%payment%'     then 'payment'
            when tags like '%e-signature%' then 'e-signature'
            when tags like '%documents%'   then 'documents'
            when tags like '%onboarding%'  then 'onboarding'
            when tags like '%fund-info%'   then 'fund-info'
            when tags like '%portal%'      then 'portal'
            when tags like '%access%'      then 'access'
            when tags like '%account%'     then 'account'
            else 'other'
        end                                              as primary_topic,

        case
            when tags like '%kyc%'         then 'compliance'
            when tags like '%onboarding%'  then 'compliance'
            when tags like '%commitment%'  then 'close_critical'
            when tags like '%payment%'     then 'close_critical'
            when tags like '%e-signature%' then 'close_critical'
            when tags like '%documents%'   then 'close_critical'
            when tags like '%fund-info%'   then 'pre_investment'
            when tags like '%portal%'      then 'platform'
            when tags like '%access%'      then 'platform'
            when tags like '%account%'     then 'platform'
            else 'other'
        end                                              as topic_category
    from source
)

select * from cleaned
