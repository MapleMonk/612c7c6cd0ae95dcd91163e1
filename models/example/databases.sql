{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.ZOUK_SWIGGY_ADS_FACT_ITEMS AS select \'SWIGGY\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(TOTAL_CTR as FLOAT64) as CTR ,cast(COALESCE(SAFE.PARSE_DATE(\'%d-%m-%Y\', METRICS_DATE),SAFE.PARSE_DATE(\'%d/%m/%Y\', METRICS_DATE)) as date) Date ,SAFE_CAST(0 as FLOAT64) as Views ,SAFE_CAST(TOTAL_CLICKS as FLOAT64) as Clicks ,Campaign_ID ,SAFE_CAST(TOTAL_IMPRESSIONS as FLOAT64) as Impressions ,Product_Name ,Campaign_Name ,SAFE_CAST(TOTAL_ROI as FLOAT64) * SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as ad_sales ,SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as spend from `MapleMonk.ZOUK_S3_SWIGGY_INSTAMART_ADS` where METRICS_DATE <> \'\';",
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
            