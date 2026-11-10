{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.missing_product_mapping as select distinct product_id, marketplace from Maplemonk.philips_db_sales_consolidated where sku is null",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PHILIPS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            