{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace TABLE rpsg_db.maplemonk.three60_amplitude_order_by_source as select event_time::date as date , event_properties:transactionId ::string as transactionId ,upper((USER_PROPERTIES:\"initial_referring_domain\")::string) as referring_domain ,upper(coalesce(USER_PROPERTIES:\"utm_source\",USER_PROPERTIES:\"initial_utm_source\")::string) as source ,upper(coalesce(USER_PROPERTIES:\"utm_medium\",USER_PROPERTIES:\"initial_utm_medium\")::string) as medium ,upper(coalesce(USER_PROPERTIES:\"utm_campaign\",USER_PROPERTIES:\"initial_utm_campaign\")::string) as campaign FROM rpsg_db.maplemonk.amplitude_drv_events where event_type = \'Purchase\' and transactionid is not null and (not(lower(source) = \'empty\' and lower(medium) = \'empty\') or lower(referring_domain) like \'%google%\') order by event_time::date desc",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            