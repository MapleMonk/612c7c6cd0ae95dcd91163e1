{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.REF_CATEGORY_PRICE_SLABS AS SELECT UPPER(TRIM(CATEGORY)) AS CATEGORY, MIN(PRICE) AS MIN_PRICE, PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY PRICE) AS P25_PRICE, PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY PRICE) AS P50_PRICE, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY PRICE) AS P75_PRICE, MAX(PRICE) AS MAX_PRICE, CURRENT_TIMESTAMP() AS UPDATED_AT FROM SNITCH_DB.MAPLEMONK.AVAILABILITY_MASTER_V2 WHERE CATEGORY IS NOT NULL AND PRICE > 0 GROUP BY UPPER(TRIM(CATEGORY));",
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
            