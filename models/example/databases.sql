{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `maplemonk.zouk_grand_vegan_sale` AS SELECT Order_Date, Marketplace, CASE WHEN COUNT(DISTINCT Order_ID) > 0 THEN COUNT(DISTINCT Order_ID) ELSE ROUND(SUM(Quantity) / 1.5) END AS Order_Count, SUM(Quantity) AS Total_Quantity FROM `maplemonk.zouk_secondary_sales_consolidated` WHERE Order_Date BETWEEN DATE(\'2024-11-01\') AND DATE(\'2024-12-03\') GROUP BY Order_Date, Marketplace ORDER BY Order_Date, Marketplace;",
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
            