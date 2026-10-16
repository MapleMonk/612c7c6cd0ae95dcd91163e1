{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.clickstream_pareto AS WITH sku_level AS ( SELECT REQUEST_DATE::date AS request_date, CATEGORY, SKU_GROUP, SUM(IMPRESSION) AS impressions, SUM(CLICKS) AS clicks, SUM(ADD_TO_CART) AS atc, SUM(PAYMENT) AS payments, SUM(PURCHASE) AS purchases FROM snitch_db.maplemonk.clickstream_metabase WHERE SKU_GROUP IS NOT NULL GROUP BY 1, 2, 3 ), ranked AS ( SELECT *, SUM(purchases) OVER (PARTITION BY request_date, CATEGORY) AS cat_total_purchases, SUM(purchases) OVER (PARTITION BY request_date, CATEGORY ORDER BY purchases DESC, clicks DESC, SKU_GROUP ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_purchases, ROW_NUMBER() OVER (PARTITION BY request_date, CATEGORY ORDER BY purchases DESC, clicks DESC, SKU_GROUP) AS sku_rank, COUNT(*) OVER (PARTITION BY request_date, CATEGORY) AS skus_live FROM sku_level ) SELECT request_date, CATEGORY, SKU_GROUP, impressions, clicks, atc, payments, purchases, clicks / NULLIF(impressions, 0) AS ctr, atc / NULLIF(clicks, 0) AS click_to_atc, purchases / NULLIF(impressions, 0) AS purchase_per_impression, sku_rank, skus_live, cum_purchases / NULLIF(cat_total_purchases, 0) AS cum_pct_purchases, CASE WHEN purchases = 0 THEN \'No sale\' WHEN cum_purchases - purchases < 0.8 * cat_total_purchases THEN \'Top 80%\' ELSE \'Bottom 20%\' END AS pareto_bucket FROM ranked; CREATE OR REPLACE TABLE snitch_db.maplemonk.clickstream_pareto_summary AS SELECT request_date, CATEGORY, SUM(purchases) AS total_purchases, MAX(skus_live) AS skus_live, COUNT_IF(purchases > 0) AS skus_sold, COUNT_IF(pareto_bucket = \'Top 80%\') AS skus_in_top_80, COUNT_IF(pareto_bucket = \'Top 80%\') / NULLIF(MAX(skus_live), 0) AS pct_skus_in_top_80, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr, SUM(atc) AS atc, SUM(purchases) / NULLIF(SUM(impressions), 0) AS purchase_per_impression FROM snitch_db.maplemonk.clickstream_pareto GROUP BY 1, 2;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            