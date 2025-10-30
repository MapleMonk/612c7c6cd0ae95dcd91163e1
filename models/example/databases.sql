{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.monthly_summary_performance_marketing as ( SELECT DATE_TRUNC(\'MONTH\', date) AS date, sum(gross_sales) AS Gross_Sales, sum(ifnull(marketing_spend,0)) AS MARKETING_SPEND, sum(ifnull(influ_spend,0)) as influ_spend, sum(ifnull(total_spend,0)) as total_spend, sum(ifnull(offline_spend,0)) as offline_spend, sum(ifnull(brand_spend,0)) as brand_spend, DIV0( SUM(IFNULL(gross_sales, 0)), NULLIF(SUM(IFNULL(marketing_spend, 0)), 0) ) AS ROAS, DIV0( SUM(IFNULL(gross_sales, 0)), NULLIF(SUM(IFNULL(total_spend, 0)), 0) ) AS Total_ROAS, sum(total_discount) AS TOTAL_DISCOUNT, div0((sum(total_spend)+sum(total_discount))/sum(gross_sales), 1) AS \"Discount + Spend / Sales\", div0((sum(marketing_spend)+sum(total_discount))/sum(gross_sales), 1) AS \"Discount + Marketing Spend / Sales\", sum(return_value) AS Return_Value, div0(sum(return_value)/sum(gross_sales), 1) AS Return_percentage, div0(sum(ifnull(total_discount, 0)),(sum(ifnull(total_discount, 0))+sum(ifnull(gross_sales, 0))))*100 AS Discount_percentage, CASE WHEN DATE_TRUNC(\'month\', date) = DATE_TRUNC(\'month\', CURRENT_DATE) THEN DIV0( SUM(gross_sales), NULLIF(DAY(CURRENT_DATE) - 1, 0) ) * DAY(LAST_DAY(CURRENT_DATE)) ELSE SUM(gross_sales) END AS Projection_Sales, CASE WHEN date_trunc(\'month\', date) = date_trunc(\'month\', current_date) THEN (sum(case when date <= current_date -2 then ifnull(WEB_SESSIONS,0)+ifnull(WEB2_SESSIONS,0)+ifnull(APPBREW_SESSIONS,0)+ifnull(APP_IOS_SESSIONS,0)+ifnull(APP_ANDROID_SESSIONS,0) end) / (day(current_date) - 2)) * day(last_day(current_date)) ELSE sum(ifnull(WEB_SESSIONS,0)+ifnull(WEB2_SESSIONS,0)+ifnull(APPBREW_SESSIONS,0)+ifnull(APP_IOS_SESSIONS,0)+ifnull(APP_ANDROID_SESSIONS,0)) END AS Projection_Traffic FROM snitch_db.maplemonk.sales_cost_source_snitch GROUP BY DATE_TRUNC(\'MONTH\', date) )",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            