{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_DB.MAPLEMONK.slashed_price as SELECT D.sku_group, price, compared_price, order_date, C.inventory_available, D.quantity, D.gross_sales FROM (select * from ( select REVERSE(SUBSTRING(REVERSE(REPLACE(A.value:sku, \'\"\', \'\')), CHARINDEX(\'-\', REVERSE(REPLACE(A.value:sku, \'\"\', \'\'))) + 1)) AS sku_group , REPLACE(A.value:price, \'\"\', \'\') AS price, REPLACE(A.value:compare_at_price, \'\"\', \'\') AS compared_price, row_number() over(partition by sku_group order by UPDATED_AT desc) rw from snitch_db.maplemonk.SHOPIFY_ALL_PRODUCTS P,LATERAL FLATTEN(INPUT => P.VARIANTS)A ) where rw=1 and compared_price is not null and compared_price-price>0 )P LEFT JOIN ( select * from (select sku_group, inventory_available, row_number() over(partition by sku_group order by inventory_available desc )rw from snitch_db.maplemonk.inventory_summary_snitch ) where rw=1 )C ON p.sku_group = C.sku_group LEFT JOIN (select sku_group, order_timestamp::date order_date, sum(quantity) as Quantity, sum(gross_sales) as gross_sales from snitch_db.maplemonk.fact_items_snitch group by 1,2 ) D ON C.sku_group = D.sku_group",
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
                        