{{ config(
            materialized='table',
                post_hook={
                    "sql": "WITH plus_skus AS ( SELECT DISTINCT SKU_GROUP, CATEGORY FROM snitch_db.maplemonk.category_overall_data WHERE UPPER(TRIM(SKU_GROUP)) LIKE \'4B%\' OR UPPER(TRIM(SKU_GROUP)) LIKE \'4MBG%\' ), plus_returns AS ( SELECT r.ORDER_DATE AS DATE, SUM(r.RETURN_QTY_REAL) AS RETURN_QTY, SUM(r.RETURN_VALUE) AS RETURN_VALUE, SUM(r.SALES_QTY) AS SALES_QTY, SUM(r.SALES_VALUE) AS SALES_VALUE, ROUND( SUM(r.RETURN_QTY_REAL) * 100.0 / NULLIF(SUM(r.SALES_QTY), 0), 2 ) AS RETURN_PCT_QTY, ROUND( SUM(r.RETURN_VALUE) * 100.0 / NULLIF(SUM(r.SALES_VALUE), 0), 2 ) AS RETURN_PCT_VALUE FROM snitch_db.maplemonk.RETURN_FACT_ITEMS r INNER JOIN plus_skus p ON UPPER(TRIM(r.SKU_GROUP)) = UPPER(TRIM(p.SKU_GROUP)) GROUP BY r.ORDER_DATE ) SELECT * FROM plus_returns ORDER BY DATE DESC;",
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
            