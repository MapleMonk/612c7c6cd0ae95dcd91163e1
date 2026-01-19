{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.sfg_inventory_fact_items as with sfg_sku_sales as ( select a.order_Date , a.sku , coalesce(b.mapped_sfg, a.sku) as sfg_sku , sum(a.quantity) parent_quantity , sum(coalesce(a.quantity*cast(b.mapped_sfg_sku as float64), a.quantity)) sfg_quantity from MAPLEMONK.P_TAL_DB_sales_consolidated a left join (select sku, mapped_sfg, mapped_sfg_sku from ( select sku, mapped_sfg, mapped_sfg_sku, row_number() over (partition by sku, mapped_sfg order by 1) rw from MapleMonk.google_sheet_FG_Bundle_SFG ) where rw = 1 ) b on trim(lower(a.sku)) = trim(lower(b.sku)) where a.sku is not null group by 1,2,3 ) select coalesce(a.sfg_sku, b.sfg_sku) sfg_sku , a.sfg_quantity_l7 , a.sfg_quantity_l30 , a.sfg_quantity_l60 , a.sfg_quantity_l90 , b.available_inventory , b.material_name , b.category , b.updated_at inventory_updated_At from (select sfg_sku , sum(case when DATE_DIFF(current_Date, order_Date, day) <=7 and ordeR_Date < current_Date then sfg_quantity end) sfg_quantity_l7 , sum(case when DATE_DIFF(current_Date, order_Date, day) <=30 and ordeR_Date < current_Date then sfg_quantity end) sfg_quantity_l30 , sum(case when DATE_DIFF(current_Date, order_Date, day) <=60 and ordeR_Date < current_Date then sfg_quantity end) sfg_quantity_l60 , sum(case when DATE_DIFF(current_Date, order_Date, day) <=90 and ordeR_Date < current_Date then sfg_quantity end) sfg_quantity_l90 from sfg_sku_sales group by 1 ) a full outer join ( select product_sku_code sfg_sku, category, product_name material_name, cast(current_stock as float64) available_inventory, null updated_At from (select *, row_number() over (partition by Product_SKU_CODE) rw from maplemonk.google_sheet_sfg_inventory ) where rw = 1 ) b on trim(lower(a.sfg_sku)) = trim(lower(b.sfg_sku)) ;",
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
            