{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.daily_inventory as select skucode sku ,facility ,inventory ,b.name ,category ,sub_Category ,size ,style_no ,image_link ,selling_price ,mrp from MAPLEMONK.Unicommerce_Unicommerce_get_inventory_snapshot a left join (select * from (select sku , title name , category , sub_category , size , style_no , image_link , Selling_Price , mrp , row_number() over (partition by replace(sku,\' \',\'\') order by 1) rw from maplemonk.mapping_sku_master) where rw = 1 ) b on a.skucode = b.sku ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            