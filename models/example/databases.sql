{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or replace Table maplemonk.cc_other_marketplaces_spends_fact_items as WITH parsed_data AS ( SELECT brand, market, Currency, Type_of_Spend as Channel, DATE(SUBSTR(month, 1, 7) || \'-01\') AS month_start, SAFE_CAST(REPLACE(REPLACE(Total_Cost, \'£\', \'\'), \',\', \'\') AS FLOAT64) AS Total_Cost FROM `MAPLEMONK.cc_Backlinking_SEO` UNION ALL SELECT null as domain, null as market, \'GBP\' AS Currency, \'BB_Influe\' AS Channel, DATE(PARSE_DATE(\'%d-%m-%Y\', month)) AS month_start, SAFE_CAST(REPLACE(REPLACE(Cost_GBP, \'£\', \'\'), \',\', \'\') AS FLOAT64) AS Total_Cost FROM `MAPLEMONK.cc_BB_Influee` UNION ALL SELECT domain, market, \'GBP\' AS Currency, Portal AS Channel, DATE(PARSE_DATE(\'%d/%m/%Y\', month)) AS month_start, SAFE_CAST(REPLACE(REPLACE(Cost_GBP, \'£\', \'\'), \',\', \'\') AS FLOAT64) AS Total_Cost FROM `MAPLEMONK.cc_portal` UNION ALL SELECT brand, market, \'GBP\' AS Currency, \'SNAPCHAT\' AS Channel, DATE(PARSE_DATE(\'%d/%m/%Y\', month)) AS month_start, SAFE_CAST(REPLACE(REPLACE(Cost_GBP, \'£\', \'\'), \',\', \'\') AS FLOAT64) AS Total_Cost FROM `MAPLEMONK.cc_snapchat` UNION ALL SELECT brand, market, \'GBP\' AS Currency, \'TIKTOK\' AS Channel, DATE(PARSE_DATE(\'%d/%m/%Y\', month)) AS month_start, SAFE_CAST(REPLACE(REPLACE(Amount_in_GBP, \'£\', \'\'), \',\', \'\') AS FLOAT64) AS Total_Cost FROM `MAPLEMONK.cc_tiktok` ), expanded_days AS ( SELECT brand, market, Currency, Channel, month_start, DATE_ADD(month_start, INTERVAL day_num - 1 DAY) AS day, Total_Cost, LAST_DAY(month_start) AS month_end FROM parsed_data, UNNEST(GENERATE_ARRAY(1, EXTRACT(DAY FROM LAST_DAY(month_start)))) AS day_num ) SELECT brand, market, Currency, Channel, day as date, Total_Cost / EXTRACT(DAY FROM month_end) AS Daily_Cost FROM expanded_days ;",
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
            