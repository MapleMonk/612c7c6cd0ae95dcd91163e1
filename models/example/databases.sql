{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.PLUS_ROAS_DAILY AS WITH plus_sales AS ( SELECT TO_DATE(\"DATE\") AS sales_date, SUM(COALESCE(GROSS_SALES, 0)) AS plus_sales FROM SNITCH_DB.MAPLEMONK.HORIZONTAL_SALES_CATEGORIES WHERE UPPER(SKU_GROUP) LIKE \'4MBG%\' OR UPPER(SKU_GROUP) LIKE \'4B%\' GROUP BY 1 ), plus_spend AS ( SELECT TO_DATE(\"DATE\") AS spend_date, SUM(COALESCE(SPEND, 0)) AS plus_ad_spend FROM SNITCH_DB.MAPLEMONK.MARKETING_CONSOLIDATED_SNITCH WHERE LOWER(COALESCE(AD_NAME, \'\')) LIKE \'%plus%\' OR LOWER(COALESCE(ADSET_NAME, \'\')) LIKE \'%plus%\' OR LOWER(COALESCE(CAMPAIGN_NAME, \'\')) LIKE \'%plus%\' GROUP BY 1 ), daily_roas AS ( SELECT COALESCE(s.sales_date, a.spend_date) AS date, COALESCE(s.plus_sales, 0) AS plus_sales, COALESCE(a.plus_ad_spend, 0) AS plus_ad_spend FROM plus_sales s FULL OUTER JOIN plus_spend a ON s.sales_date = a.spend_date ) SELECT date, ROUND(plus_sales, 2) AS plus_sales, ROUND(plus_ad_spend, 2) AS plus_ad_spend, ROUND( plus_sales / NULLIF(plus_ad_spend, 0), 2 ) AS plus_roas, ROUND( 100.0 * ( plus_sales - LAG(plus_sales) OVER (ORDER BY date) ) / NULLIF( LAG(plus_sales) OVER (ORDER BY date), 0 ), 2 ) AS sales_dod_pct, ROUND( 100.0 * ( plus_ad_spend - LAG(plus_ad_spend) OVER (ORDER BY date) ) / NULLIF( LAG(plus_ad_spend) OVER (ORDER BY date), 0 ), 2 ) AS spend_dod_pct FROM daily_roas ORDER BY date DESC;",
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
            