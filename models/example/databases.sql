{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_inventory_sales_fact_items as with grn as ( select grn_created_date, sku, sum(received_quantity) inwarded_quantity from maplemonk.saadaa_grn_fact_items group by 1,2 ), sales as ( select date(order_date) order_date, commonsku, sum(ifnull(quantity,0)) sold_quantity from maplemonk.saadaa_sales_consolidated where lower(GROSS_ORDER_TYPE) like \'%sale%\' group by 1,2 ) select g.grn_created_date, g.sku, sm.product_id, sm.product_name, sm.category, sm.size, sm.color, sm.item_category, sm.categorytype, sm.gender, sm.Product_Launch_Date, sm.Product_Variant, sm.Sub_category, g.inwarded_quantity, ifnull(s.sold_quantity,0) sold_quantity from grn g left join sales s on date(g.grn_created_date) = date(s.order_date) and lower(g.sku) = lower(s.commonsku) left join ( select commonsku, product_id, product_name, category, size, color, item_category, categorytype, gender, Product_Launch_Date, Product_Variant, Sub_category from `MAPLEMONK.saadaa_final_sku_master` qualify row_number() over (partition by commonsku order by 1) = 1 ) sm on lower(sm.commonsku) = lower(g.sku)",
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
            