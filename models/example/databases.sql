{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_Zepto_ads_Fact_Items; create table public.anveshan_Zepto_ads_Fact_Items as SELECT CAST(t.date AS date) AS date, TRIM(TO_CHAR(CAST(t.date AS date), \'Day\')) AS day_of_week, EXTRACT(YEAR FROM CAST(t.date AS date)) AS year, EXTRACT(MONTH FROM CAST(t.date AS date)) AS month, \'ZEPTO\' AS channel, \'ZEPTO\' AS account, t.brandid, t.brandname, t.productid, t.campaign_id, t.productname, t.campaign_name, t.category, SUM(CAST(t.impressions AS DOUBLE PRECISION)) AS impressions, SUM(CAST(t.revenue AS DOUBLE PRECISION)) AS revenue, SUM(CAST(t.spend AS DOUBLE PRECISION)) AS spend, SUM(CAST(t.roas AS DOUBLE PRECISION)) AS roas, SUM(CAST(t.ctr AS DOUBLE PRECISION)) AS ctr, SUM(CAST(t.cpm AS DOUBLE PRECISION)) AS cpm, SUM(CAST(t.atc AS DOUBLE PRECISION)) AS atc, SUM(CAST(t.clicks AS DOUBLE PRECISION)) AS clicks, SUM(CAST(t.orders AS DOUBLE PRECISION)) AS orders FROM public.anveshan_zepto_sponsored_products t GROUP BY t.date, TRIM(TO_CHAR(CAST(t.date AS date), \'Day\')), EXTRACT(YEAR FROM CAST(t.date AS date)), EXTRACT(MONTH FROM CAST(t.date AS date)), t.brandid, t.brandname, t.productid, t.campaign_id, t.productname, t.campaign_name, t.category ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            