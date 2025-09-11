{{ config(
            materialized='table',
                post_hook={
                    "sql": "reate or replace table eggozdb.maplemonk.beat_tagging as select distributor_name,dealer_name,rr.code,rr.retailer_type,rr.category, rr.onboarding_status, dcu.name as dsr_name, scu.name as sr_name, socu.name as so_name, rb.beat_area, rr.beat_number,rr.area_classification from eggozdb.maplemonk.my_sql_tertiary_retailer_retailer rr left join my_sql_retailer_retailerbeat rb on rb.id = rr.retailerBeat_id left join my_sql_saleschain_salespersonprofile dsr on dsr.id= rb.DEALER_SALESREPRESENTATIVE_ID left join my_sql_saleschain_salespersonprofile sr on sr.id= rb.SALESREPRESENTATIVE_ID left join my_sql_saleschain_salespersonprofile so on so.id= rb.SALESOFFICER_ID left join my_sql_custom_auth_user dcu on dsr.user_id = dcu.id left join my_sql_custom_auth_user scu on sr.user_id = scu.id left join my_sql_custom_auth_user socu on so.user_id = socu.id ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            