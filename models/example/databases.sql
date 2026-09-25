{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table peesafe_db.maplemonk.missing_sku_mappings as select distinct product_id, marketplace from PEESAFE_DB.MAPLEMONK.PEESAFE_DB_sales_consolidated where sku_code is null;",
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
            