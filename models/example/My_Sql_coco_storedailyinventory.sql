{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table eggozdb.maplemonk.my_sql_COCO_STORE_INVENTORY as select date(date) as DATE,rr.id as Retailer_id ,rr.billing_name_of_shop ,pp.id as Product_id, CONCAT(pp.sku_count, pp.short_name) as SKU_NAME , rr.AREA_CLASSIFICATION, sum(out_qty) as OUT_QTY, sum(inv.in_qty) as IN_QTY, sum(inv.opening_qty) as OPENING_QTY, sum(inv.closing_qty) as CLOSING_QTY from eggozdb.maplemonk.MY_SQL_coco_storedailyinventory inv join eggozdb.maplemonk.MY_SQL_product_product pp on pp.id = inv.product_id join eggozdb.maplemonk.MY_SQL_retailer_retailer rr on rr.id = inv.retailer_id group by 1,2,3,4,5,6;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.MAPLEMONK.My_Sql_coco_storedailyinventory
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            