{{ config(
            materialized='table',
                post_hook={
                    "sql": "create temp table qc_inventory as select \'KNOT\' as marketplace, i.style as sku_style, i.size as sku_size, max(date(Updated_Date)) inventory_date, sum(ifnull(cast(i.inventory as int64),0)) as inventory, from maplemonk.QC_Knot_inventory i where date(Updated_Date) = (select max(date(updated_date)) from maplemonk.QC_Knot_inventory) group by 1,2,3 union all select \'NEON MARKET\' as marketplace, i.style as sku_style, i.size as sku_size, max(date(Updated_Date)) inventory_date, sum(ifnull(cast(i.inventory as int64),0)) as inventory from maplemonk.GS_Neon_Market_inventory i where date(Updated_Date) = (select max(date(updated_date)) from maplemonk.GS_Neon_Market_inventory) group by 1,2,3 union all select \'SLIKK\' as marketplace, i.style as sku_style, i.size as sku_size, max(date(parse_date(\'%d-%m-%Y\',Updated))) inventory_date, sum(ifnull(cast(i.inventory as int64),0)) as inventory from maplemonk.QC_Slikk_inventory i where date(parse_date(\'%d-%m-%Y\',Updated)) = (select max(date(parse_date(\'%d-%m-%Y\',Updated))) from maplemonk.QC_Knot_inventory) group by 1,2,3 ; create or replace table maplemonk.quick_com_inv_sales as with inventory as ( select * from qc_inventory ), sales as ( select sku_style style, sku_size size, marketplace, sum(ifnull(quantity,0)) as sold_quantity from maplemonk.izf_sales_consolidated s where lower(marketplace) like any (\'%knot%\',\'%neon%\',\'%slikk%\') and DATE(order_date) > ( select max(inventory_date) from qc_inventory i_corr where lower(i_corr.marketplace) = lower(s.marketplace) ) group by 1,2,3 ) select i.marketplace, i.sku_style, i.sku_size, i.Inventory_date, ifnull(s.sold_quantity,0) sold_quantity_10_days, i.inventory, from inventory i left join sales s on lower(trim(i.sku_style)) = lower(trim(s.style)) and lower(trim(i.sku_size)) = lower(trim(s.size)) and lower(trim(i.marketplace)) = lower(trim(s.marketplace)) ;",
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
            