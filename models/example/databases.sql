{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ecommerce_marketplace_target as select Brand, upper(marketplace) as Marketplace, cast(cast(replace(__Spends__Secondary,\'%\',\'\') as float64)/100 as float64) Spends_by_Secondary, cast(cast(replace(__Primary___Secondary,\'%\',\'\') as float64)/100 as float64) Primary_by_Secondary, cast(cast(replace(__spends___Primary_Revenue,\'%\',\'\') as float64)/100 as float64) Spends_by_Primary_Revenue, cast(replace(TOTAL_Spends_Including_GST,\',\',\'\') as int64) as TOTAL_Spends_Including_GST, cast(replace(Secondary_Sales_Including_GST,\',\',\'\') as int64) as Secondary_Sales_Including_GST, cast(replace(Revenue__Primaries__where_applicable__including_GST_,\',\',\'\') as int64) as Revenue_Primaries_where_applicable_including_GST, DATE(SAFE_CAST(CONCAT(\'20\', RIGHT(Month, 2)) AS INT64), EXTRACT(MONTH FROM PARSE_DATE(\'%b\', LEFT(Month, 3))), 1) as month from maplemonk.ecommerce_target ;",
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
            