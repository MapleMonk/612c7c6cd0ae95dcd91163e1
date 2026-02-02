{{ config(
            materialized='table',
                post_hook={
                    "sql": "create temp table qc_inventory as select * from ( select \'KNOT\' as marketplace, i.style as sku_style, i.size as sku_size, date(Updated_Date) inventory_date, ifnull(cast(i.inventory as int64),0) as inventory, row_number() over (partition by i.style,i.size,date(Updated_Date) order by date(Updated_Date) desc) as rw from maplemonk.QC_Knot_inventory i ) where rw=1 union all select * from ( select \'NEON MARKET\' as marketplace, i.style as sku_style, i.size as sku_size, date(Updated_Date) inventory_date, ifnull(cast(i.inventory as int64),0) as inventory, row_number() over (partition by i.style,i.size,date(Updated_Date) order by date(Updated_Date) desc) as rw from maplemonk.GS_Neon_Market_inventory i ) where rw=1 union all select * from ( select \'SLIKK\' as marketplace, i.style as sku_style, i.size as sku_size, date(parse_date(\'%d-%m-%Y\',Updated)) inventory_date, ifnull(cast(i.inventory as int64),0) as inventory, row_number() over (partition by i.style,i.size,date(parse_date(\'%d-%m-%Y\',Updated)) order by date(parse_date(\'%d-%m-%Y\',Updated)) desc) as rw from maplemonk.QC_Slikk_inventory i ) where rw = 1 ; create or replace table maplemonk.quick_com_inv_sales as with inventory as ( select * from qc_inventory ), sales as ( select s.sku_style as style, s.sku_size as size, s.marketplace, s.order_date, ifnull(s.quantity, 0) as quantity from maplemonk.izf_sales_consolidated s where lower(s.marketplace) like any (\'%knot%\', \'%neon%\', \'%slikk%\') ), aggregated_sales as ( select s.style, s.size, s.marketplace, sum(s.quantity) as sold_quantity from sales s inner join inventory i on lower(trim(s.style)) = lower(trim(i.sku_style)) and lower(trim(s.size)) = lower(trim(i.sku_size)) and lower(trim(s.marketplace)) = lower(trim(i.marketplace)) where date(s.order_date) >= i.inventory_date group by 1, 2, 3 ) select i.marketplace, i.sku_style, i.sku_size, i.inventory_date, ifnull(s.sold_quantity, 0) as sold_quantity_since_update, i.inventory from inventory i left join aggregated_sales s on lower(trim(i.sku_style)) = lower(trim(s.style)) and lower(trim(i.sku_size)) = lower(trim(s.size)) and lower(trim(i.marketplace)) = lower(trim(s.marketplace)) ;",
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
            