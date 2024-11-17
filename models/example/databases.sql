{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.monthly_summary_performance_marketing as ( SELECT DATE_TRUNC(\'MONTH\', date) AS date, sum(gross_sales) AS \"Gross Sales\", sum(marketing_spend) AS \"Marketing spend\", div0(sum(gross_sales)/sum(marketing_spend), 1) AS \"ROAS\", sum(total_discount) AS \"Total discount\", div0((sum(marketing_spend)+sum(total_discount))/sum(gross_sales), 1) AS \"Discount + Spend / Sales\", sum(return_value) AS \"Return Value\", div0(sum(return_value)/sum(gross_sales), 1) AS \"Return %\", div0(sum(ifnull(total_discount, 0)),(sum(ifnull(total_discount, 0))+sum(ifnull(gross_sales, 0))))*100 AS \"Discount %\", CASE WHEN date_trunc(\'month\', date) = date_trunc(\'month\', current_date) THEN (sum(gross_sales) / (day(current_date) - 1)) * day(last_day(current_date)) ELSE sum(gross_sales) END AS \"Projection\" FROM snitch_db.maplemonk.sales_cost_source_snitch GROUP BY DATE_TRUNC(\'MONTH\', date) ORDER BY max(date) DESC );",
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
            