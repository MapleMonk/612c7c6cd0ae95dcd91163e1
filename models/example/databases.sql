{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE or replace TABLE prolicious-wh.MapleMonk.prolicious_swiggy_ads_Fact_items AS SELECT \'SWIGGY\' AS channel, \'PRODUCT ADS\' AS ad_type, upper(account_id) as account_id, cast(replace(total_ctr,\'%\',\'\') AS FLOAT64)/100 AS ctr, cast(bidding_type as string) AS AD_GROUP_NAME, cast(null as string) AS KEYWORD, cast(null as string) AS CITY, date(metrics_date) AS date, cast(0 AS FLOAT64) AS views, cast(total_clicks AS FLOAT64) AS clicks, campaign_id, cast(total_impressions AS FLOAT64) AS impressions, cast(total_conversions AS int64) AS conversions, s.campaign_name, cast(total_gmv AS FLOAT64) AS ad_sales, cast(total_budget_burnt AS FLOAT64) AS spend, cast(TOTAL_A2C AS int64) AS atc, \'Manual\' AS type FROM prolicious-wh.maplemonk.Prolicious_Swiggy_campaign_x_report s ;",
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
            