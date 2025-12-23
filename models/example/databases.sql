{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_swiggy_ads_fact_items; CREATE TABLE public.anveshan_swiggy_ads_fact_items AS SELECT \'SWIGGY\' AS channel, \'PRODUCT ADS\' AS ad_type, TRY_CAST(total_ctr AS FLOAT) AS ctr, COALESCE( TRY_CAST(TRIM(metrics_date) AS DATE), TO_DATE(TRIM(metrics_date), \'DD-MM-YYYY\'), TO_DATE(TRIM(metrics_date), \'YYYY-MM-DD\'), TO_DATE(TRIM(metrics_date), \'DD-MM-YY\'), TO_DATE(TRIM(metrics_date), \'MM/DD/YYYY\') ) AS date, TRY_CAST(0 AS FLOAT) AS views, TRY_CAST(total_clicks AS FLOAT) AS clicks, campaign_id, TRY_CAST(total_impressions AS FLOAT) AS impressions, ba.product_name, campaign_name, TRY_CAST(total_roi AS FLOAT) * TRY_CAST(total_budget_burnt AS FLOAT) AS ad_sales, TRY_CAST(total_budget_burnt AS FLOAT) AS spend, \'Manual\' AS type FROM public.swiggy_ads_anveshan_s3_instamart_ads ba ;",
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
            