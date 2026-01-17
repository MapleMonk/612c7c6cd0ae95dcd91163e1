{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.plus_Size_web_sales_cost_source as with sales as ( select ORDER_TIMESTAMP::DATE AS ORDER_DATE, COUNT(DISTINCT COMBINED_ORDER_NAME) AS ORDERS, SUM(QUANTITY) AS QUANTITY, SUM(GROSS_SALES) AS GROSS_SALES, abs(SUM(CASE WHEN DISCOUNT_CODE LIKE \'%rms%\' THEN 0 ELSE DISCOUNT END)) AS DISCOUNT, abs(ifnull(SUM(CASE WHEN DISCOUNT_CODE LIKE \'%rms%\' THEN 0 WHEN NEW_CUSTOMER_FLAG = \'New\' THEN DISCOUNT END),0)) AS NEW_CUSTOMER_DISCOUNT, ifnull(SUM(CASE WHEN DISCOUNT_CODE LIKE \'%rms%\' THEN 0 WHEN NEW_CUSTOMER_FLAG <> \'New\' THEN DISCOUNT END),0) AS REPEAT_CUSTOMER_DISCOUNT, COUNT(DISTINCT CASE WHEN NEW_CUSTOMER_FLAG = \'New\'THEN CUSTOMER_ID END) AS NEW_CUSTOMER, ifnull(COUNT(DISTINCT CASE WHEN NEW_CUSTOMER_FLAG <> \'New\' THEN CUSTOMER_ID END),0) AS REPEAT_CUSTOMER, ifnull(SUM(CASE WHEN NEW_CUSTOMER_FLAG = \'New\' THEN GROSS_SALES END),0) AS NEW_CUSTOMER_SALES, ifnull(SUM(CASE WHEN NEW_CUSTOMER_FLAG <> \'New\' THEN GROSS_SALES END),0) AS REPEAT_CUSTOMER_SALES, COUNT(DISTINCT CASE WHEN NEW_CUSTOMER_FLAG = \'New\'THEN COMBINED_ORDER_NAME END) AS NEW_CUSTOMER_ORDERS, COUNT(DISTINCT CASE WHEN NEW_CUSTOMER_FLAG <> \'New\' THEN COMBINED_ORDER_NAME END) AS REPEAT_CUSTOMER_ORDERS, COUNT(DISTINCT CASE WHEN PAYMENT_CHANNEL = \'COD\' THEN COMBINED_ORDER_NAME END) AS COD_ORDERS FROM SNITCH_DB.MAPLEMONK.fact_items_snitch WHERE LOWER(TAGS) LIKE \'%plus%\' group by 1 ), sessions as ( SELECT CLICK_DATE, SUM(SESSION_START) as sessions, FROM SNITCH_DB.maplemonk.GA4_PLUS_SIZE_DATA GROUP BY 1 ) select a.*, b.sessions from sales a left join sessions b on a.order_date = b.click_date",
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
            