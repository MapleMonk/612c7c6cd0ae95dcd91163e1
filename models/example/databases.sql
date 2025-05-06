{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ras_luxury_sales_consolidated as select * from `MAPLEMONK.ras_oil_sales_consolidated` union all select * from `MAPLEMONK.ras_mini_sales_consolidated` union all select * from `MAPLEMONK.ras_dot_in_sales_consolidated` union all select * from `MAPLEMONK.moody_sales_consolidated` union all select * from `MAPLEMONK.ras_easyecom_sales_consolidated` ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            