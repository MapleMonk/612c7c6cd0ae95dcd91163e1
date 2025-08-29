{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.zouk_print_ranking_Report` AS With AggregatedData AS ( SELECT \'PRINT\' AS dimension, PRINT AS dimension_value, p.statename as State, p.Region, SUM(CASE WHEN Order_Date >= DATE_SUB(cast(current_datetime(\"Asia/Kolkata\") as date), INTERVAL 30 DAY) THEN SELLING_PRICE ELSE 0 END) AS sales_30_days, SUM(CASE WHEN Order_Date >= DATE_SUB(cast(current_datetime(\"Asia/Kolkata\") as date), INTERVAL 60 DAY) THEN SELLING_PRICE ELSE 0 END) AS sales_60_days, SUM(CASE WHEN Order_Date >= DATE_SUB(cast(current_datetime(\"Asia/Kolkata\") as date), INTERVAL 90 DAY) THEN SELLING_PRICE ELSE 0 END) AS sales_90_days FROM `MapleMonk.zouk_secondary_sales_consolidated` s LEFT JOIN `MapleMonk.Zouk_Updated_India_Post_Data` p ON s.pincode = p.pincode WHERE Order_Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) GROUP BY PRINT,state,Region ) SELECT dimension, dimension_value, State, Region, RANK() OVER (PARTITION BY dimension, state ORDER BY sales_30_days DESC) AS rank_30_state, RANK() OVER (PARTITION BY dimension, state ORDER BY sales_60_days DESC) AS rank_60_state, RANK() OVER (PARTITION BY dimension, state ORDER BY sales_90_days DESC) AS rank_90_state, RANK() OVER (PARTITION BY dimension, region ORDER BY sales_30_days DESC) AS rank_30_region, RANK() OVER (PARTITION BY dimension,region ORDER BY sales_60_days DESC) AS rank_60_region, RANK() OVER (PARTITION BY dimension, region ORDER BY sales_90_days DESC) AS rank_90_region, FROM AggregatedData ORDER BY dimension;",
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
            