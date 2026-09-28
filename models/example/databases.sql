{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.zouk_pyramid_consolidated` AS SELECT PDP, Month, Units, COMMONSKU, Conversion_Rate, PRODUCT_CATEGORY, \'Myntra\' AS Channel FROM `MapleMonk.zouk_pyramid_Myntra` UNION ALL SELECT PDP, Month, orders AS Units, COMMONSKU, Conversion_Rate, PRODUCT_CATEGORY, \'Flipkart\' AS Channel FROM `MapleMonk.zouk_pyramid_Flipkart` UNION ALL SELECT GV AS PDP, Month, Qty AS Units, COMMONSKU, Conversion_Rate, PRODUCT_CATEGORY, \'Amazon\' AS Channel FROM `MapleMonk.zouk_pyramid_Amazon` ;",
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
            