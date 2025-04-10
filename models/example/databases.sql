{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.Ras_Luxury_Sales_Cost_Source as select * from `MAPLEMONK.Moody_Sales_Cost_Source` union all select * from `MAPLEMONK.Ras_Oil_Sales_Cost_Source` union all select * from `MAPLEMONK.ras_dot_in_Sales_Cost_Source` union all select * from `MAPLEMONK.ras_mini_and_easyecom_Sales_Cost_Source`;",
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
            