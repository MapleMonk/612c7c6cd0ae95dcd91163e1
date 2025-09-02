{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ecommerce_marketplace_target as SELECT Brand, Marketplace, CAST(FORMAT_DATE(\'%Y-%m-%d\',LAST_DAY(PARSE_DATE(\'%b%y\', SUBSTR(Month, 1, 3) || RIGHT(Month, 2))))AS date) AS month, CAST(REPLACE(Revenue__Primaries__where_applicable__including_GST_, \',\', \'\') AS INT64) AS Revenue_Primaries_GST, CAST(REPLACE(Secondary_Sales_Including_GST, \',\', \'\') AS INT64) AS Secondary_Sales_GST, CAST(REPLACE(Discount_cccogs, \',\', \'\') AS INT64) AS Discount_cccogs, CAST(REPLACE(Discount___to_MRP, \',\', \'\') AS FLOAT64) AS Discount_Percent_to_MRP, SAFE_CAST(REPLACE(Total_Exps_Onsite, \',\', \'\') AS INT64) AS Total_Exps_Onsite, SAFE_CAST(REPLACE(Commission, \',\', \'\') AS INT64) AS Commission, SAFE_CAST(REPLACE(Retention, \',\', \'\') AS INT64) AS Retention, CAST(REPLACE(Primary_Sale, \',\', \'\') AS INT64) AS Primary_Sales, cast(replace(Secondary_Sales_Including_GST,\',\',\'\') as int64)*0.04 as Offsite_R_and_R_Spends FROM `MAPLEMONK.Google_Sheets_Target`;",
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
            