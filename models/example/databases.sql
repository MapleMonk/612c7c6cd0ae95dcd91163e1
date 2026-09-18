{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table PEESAFE_DB.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from PEESAFE_DB.MAPLEMONK.Shopify_pee_safe_UTM_Parameters UNION select * from PEESAFE_DB.MAPLEMONK.Shopify_shop_furr_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PEESAFE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            