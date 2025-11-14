{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.quick_com_inv_sales as select \'KNOT\' as marketplace, i.style as sku_style, i.size as sku_size, \'2025-11-13\' as Inventory_date, ifnull(s.sold_quantity,0) sold_quantity_10_days, sum(ifnull(cast(i.inventory as int64),0)) as inventory, from maplemonk.izf_gs_knot_inventory i left join (select sku_style style, sku_size size, sum(ifnull(quantity,0)) as sold_quantity from maplemonk.izf_sales_consolidated where lower(marketplace) like \'%knot%\' and DATE(order_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) group by 1,2) s on lower(trim(i.style)) = lower(trim(s.style)) and lower(trim(i.size)) = lower(trim(s.size)) group by 1,2,3,4,5 ;",
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
            