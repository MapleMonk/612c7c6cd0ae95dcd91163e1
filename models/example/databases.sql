{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_DB.MAPLEMONK.slashed_price as SELECT DISTINCT REVERSE(SUBSTRING(REVERSE(REPLACE(A.value:sku, \'\"\', \'\')), CHARINDEX(\'-\', REVERSE(REPLACE(A.value:sku, \'\"\', \'\'))) + 1)) AS sku_group, REPLACE(A.value:price, \'\"\', \'\') AS price, REPLACE(A.value:compare_at_price, \'\"\', \'\') AS compared_price, C.inventory_available, sum(D.quantity) AS QTY, sum(D.gross_sales) AS SALES, FROM snitch_db.maplemonk.SHOPIFY_ALL_PRODUCTS P CROSS JOIN LATERAL FLATTEN(INPUT => P.VARIANTS) A LEFT JOIN snitch_db.maplemonk.inventory_summary_snitch C ON REVERSE(SUBSTRING(REVERSE(REPLACE(A.value:sku, \'\"\', \'\')), CHARINDEX(\'-\', REVERSE(REPLACE(A.value:sku, \'\"\', \'\'))) + 1)) = C.sku_group LEFT JOIN snitch_db.maplemonk.fact_items_snitch D ON C.sku_group = D.sku_group WHERE (CAST(REPLACE(A.value:compare_at_price, \'\"\', \'\') AS FLOAT) - CAST(REPLACE(A.value:price, \'\"\', \'\') AS FLOAT)) > 0 and inventory_available>0 Group by 1,2,3,4;",
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
                        