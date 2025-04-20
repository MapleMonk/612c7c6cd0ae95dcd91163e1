{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT order_date, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM upurfit_db.MAPLEMONK.upurfit_db_sales_consolidated GROUP BY 1, 2",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from upurfit_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            