{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE ftune-wh.maplemonk.ftune_wh_AD_PERFORMANCE AS WITH shopify AS ( SELECT DATE(order_timestamp) AS date, CAST(final_utm_campaign AS STRING) AS campaign_id, cast(final_utm_term as string) AS adset_id, cast(final_utm_content as string) AS ad_id, COUNT(DISTINCT order_id) AS orders, SUM(total_sales) AS revenue, COUNT(DISTINCT CASE WHEN SHOPIFY_NEW_CUSTOMER_FLAG = \'NEW\' THEN order_id END) AS new_customer_orders, SUM(CASE WHEN SHOPIFY_NEW_CUSTOMER_FLAG = \'NEW\' THEN total_sales END) AS new_customer_revenue FROM ftune-wh.maplemonk.ftune_wh_SHOPIFY_FACT_ITEMS WHERE final_utm_campaign IS NOT NULL GROUP BY 1,2,3,4 ), marketing AS ( SELECT DATE, cast(CAMPAIGN_ID as string) as campaign_id, cast(ADSET_ID as string) as adset_id, cast(AD_ID as string) as ad_id, SUM(SPEND) AS spend, SUM(CLICKS) AS clicks, SUM(IMPRESSIONS) AS impressions FROM ftune-wh.maplemonk.ftune_wh_MARKETING_CONSOLIDATED_INTERMEDIATE GROUP BY 1,2,3,4 ) SELECT m.DATE, m.CAMPAIGN_ID, m.ADSET_ID, m.AD_ID, m.spend, m.clicks, m.impressions, IFNULL(s.orders, 0) AS shopify_attributed_orders, IFNULL(s.revenue, 0) AS shopify_attributed_revenue, IFNULL(s.new_customer_orders,0) AS new_customer_orders, IFNULL(s.new_customer_revenue,0) AS new_customer_revenue, SAFE_DIVIDE(IFNULL(s.revenue,0), m.spend) AS shopify_attributed_roas, SAFE_DIVIDE(m.spend, s.orders) AS cpa, SAFE_DIVIDE(s.revenue, s.orders) AS aov, SAFE_DIVIDE(s.orders, m.clicks) AS conversion_rate, SAFE_DIVIDE(s.new_customer_revenue, m.spend) AS new_customer_roas, SAFE_DIVIDE(m.spend, IFNULL(s.orders,0)) AS shopify_attributed_cpa, SAFE_DIVIDE(IFNULL(s.revenue,0), IFNULL(s.orders,0)) AS shopify_attributed_aov, SAFE_DIVIDE(IFNULL(s.orders,0), m.clicks) AS shopify_attributed_conversion_rate, FROM marketing m LEFT JOIN shopify s ON m.DATE = s.date AND m.CAMPAIGN_ID = s.campaign_id AND m.ADSET_ID = s.adset_id AND m.AD_ID = s.ad_id; CREATE OR REPLACE TABLE ftune-wh.maplemonk.ftune_wh_SHOPIFY_ATTRIBUTED_ORDERS AS SELECT DATE(order_timestamp) AS order_date, order_id, CAST(final_utm_campaign AS STRING) AS campaign_id, CAST(final_utm_term AS STRING) AS adset_id, CAST(final_utm_content AS STRING) AS ad_id, order_status, SUM(total_sales) AS order_revenue, MAX(SHOPIFY_NEW_CUSTOMER_FLAG) AS shopify_new_customer_flag FROM ftune-wh.maplemonk.ftune_wh_SHOPIFY_FACT_ITEMS WHERE final_utm_campaign IS NOT NULL GROUP BY 1,2,3,4,5,6;",
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
            