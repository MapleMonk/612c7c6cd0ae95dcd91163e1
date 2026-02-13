{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_swiggy_ads_fact_items; CREATE TABLE public.anveshan_swiggy_ads_fact_items AS WITH converted_dates AS ( SELECT \'SWIGGY\' AS channel, \'PRODUCT ADS\' AS ad_type, TRY_CAST(total_ctr AS FLOAT) AS ctr, AD_PROPERTY::VARCHAR AS AD_GROUP_NAME, KEYWORD::VARCHAR AS KEYWORD, CITY::VARCHAR AS CITY, CASE WHEN TRY_CAST(SUBSTRING(metrics_date, 1, 4) AS INT) BETWEEN 1900 AND 2100 THEN CAST(SUBSTRING(metrics_date, 1, 10) AS DATE) ELSE CAST( SUBSTRING(metrics_date, 7, 4) || \'-\' || SUBSTRING(metrics_date, 4, 2) || \'-\' || SUBSTRING(metrics_date, 1, 2) AS DATE ) END AS date, TRY_CAST(0 AS FLOAT) AS views, TRY_CAST(total_clicks AS FLOAT) AS clicks, campaign_id, TRY_CAST(total_impressions AS FLOAT) AS impressions, product_name, campaign_name, TRY_CAST(total_gmv AS FLOAT) AS ad_sales, TRY_CAST(total_budget_burnt AS FLOAT) AS spend, \'Manual\' AS type FROM public.swiggy_ads_anveshan_s3_instamart_ads WHERE metrics_date <> \'\' ) SELECT c.*, gi.category, gi.type_of_ads, gi.pnl_category FROM converted_dates c left join (select * from (select campaign_name, category, type_of_ads, pnl_category, row_number() over (partition by campaign_name order by category desc) as rw from public.GS_instamart_Campaign_Mapping ) where rw=1 ) gi ON upper(trim(gz.campaign_name)) = upper(z.campaign_name);",
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
            