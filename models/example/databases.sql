{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table eggozdb.maplemonk.beat_tagging as select distributor_name,dealer_name,rr.code,rr.onboarding_status,sp.management_status, cu.name, case when sp.management_status =\'DSR\' then cu.name end DSR, case when sp.management_status =\'Sales Officer\' then cu.name end SO,rb.beat_area, rr.beat_number,rr.area_classification from eggozdb.maplemonk.my_sql_tertiary_retailer_retailer rr left join my_sql_retailer_retailerbeat rb on rb.id = rr.retailerBeat_id left join my_sql_saleschain_salespersonprofile sp on sp.id= rr.salesPersonProfile_id left join my_sql_custom_auth_user cu on sp.user_id = cu.id ;",
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
            