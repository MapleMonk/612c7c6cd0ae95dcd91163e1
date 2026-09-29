{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.zouk_pyramid_consolidated` AS SELECT CAST(PDP AS INT64) AS PDP, CAST(Month AS STRING) AS Month, CAST(Units AS INT64) AS Units, CAST(COMMONSKU AS STRING) AS COMMONSKU, SAFE_CAST(REPLACE(CAST(Conversion_Rate AS STRING), \'%\', \'\') AS FLOAT64) / 100 AS Conversion_Rate, CAST(PRODUCT_CATEGORY AS STRING) AS PRODUCT_CATEGORY, CAST(\'Myntra\' AS STRING) AS Channel FROM `MapleMonk.zouk_pyramid_Myntra` UNION ALL SELECT CAST(PDP AS INT64) AS PDP, CAST(Month AS STRING) AS Month, CAST(orders AS INT64) AS Units, CAST(COMMONSKU AS STRING) AS COMMONSKU, SAFE_CAST(REPLACE(CAST(Conversion_Rate AS STRING), \'%\', \'\') AS FLOAT64) / 100 AS Conversion_Rate, CAST(PRODUCT_CATEGORY AS STRING) AS PRODUCT_CATEGORY, CAST(\'Flipkart\' AS STRING) AS Channel FROM `MapleMonk.zouk_pyramid_Flipkart` UNION ALL SELECT CAST(GV AS INT64) AS PDP, CAST(Month AS STRING) AS Month, CAST(Qty AS INT64) AS Units, CAST(COMMONSKU AS STRING) AS COMMONSKU, SAFE_CAST(REPLACE(CAST(Conversion_Rate AS STRING), \'%\', \'\') AS FLOAT64) / 100 AS Conversion_Rate, CAST(PRODUCT_CATEGORY AS STRING) AS PRODUCT_CATEGORY, CAST(\'Amazon\' AS STRING) AS Channel FROM `MapleMonk.zouk_pyramid_Amazon` ;",
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
            