{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.qc_inventory as select * from ( select \'KNOT\' as marketplace, i.style as sku_style, i.size as sku_size, date(Updated_Date) inventory_date, ifnull(cast(i.inventory as int64),0) as inventory, row_number() over (partition by i.style,i.size,date(Updated_Date) order by date(Updated_Date) desc) as rw from maplemonk.QC_Knot_inventory i ) where rw=1 union all select * from ( select \'NEON MARKET\' as marketplace, i.style as sku_style, i.size as sku_size, date(Updated_Date) inventory_date, ifnull(cast(i.inventory as int64),0) as inventory, row_number() over (partition by i.style,i.size,date(Updated_Date) order by date(Updated_Date) desc) as rw from maplemonk.GS_Neon_Market_inventory i ) where rw=1 union all select * from ( select \'SLIKK\' as marketplace, i.style as sku_style, i.size as sku_size, date(parse_date(\'%d-%m-%Y\',Updated)) inventory_date, ifnull(cast(i.inventory as int64),0) as inventory, row_number() over (partition by i.style,i.size,date(parse_date(\'%d-%m-%Y\',Updated)) order by date(parse_date(\'%d-%m-%Y\',Updated)) desc) as rw from maplemonk.QC_Slikk_inventory i ) where rw = 1 ; create or replace table maplemonk.quick_com_inv_sales as with inventory as ( select * from maplemonk.qc_inventory qualify row_number() over (partition by marketplace,sku_style,sku_size order by inventory_date desc) = 1 ), sales as ( select lower(trim(s.sku_style)) as style, lower(trim(s.sku_size)) as size, case when lower(s.marketplace) like \'%knot%\' then \'KNOT\' when lower(s.marketplace) like \'%neon%\' then \'NEON MARKET\' when lower(s.marketplace) like \'%slikk%\' then \'SLIKK\' end as marketplace, date(s.order_date) as order_date, sum(ifnull(s.quantity, 0)) as quantity from maplemonk.izf_sales_consolidated s where lower(s.marketplace) like any (\'%knot%\', \'%neon%\', \'%slikk%\') group by 1,2,3,4 ), aggregated_sales as ( select i.marketplace, i.sku_style, i.sku_size, max(i.inventory_date) inventory_date, sum(s.quantity) as sold_quantity from sales s left join inventory i on s.style = lower(trim(i.sku_style)) and s.size = lower(trim(i.sku_size)) and s.marketplace = i.marketplace and s.order_date >= i.inventory_date group by 1, 2, 3 ) select i.marketplace, i.sku_style, i.sku_size, i.inventory_date, i.inventory, ifnull(s.sold_quantity, 0) as sold_quantity_since_update from inventory i left join aggregated_sales s on i.sku_style = s.sku_style and i.sku_size = s.sku_size and i.marketplace = s.marketplace and i.inventory_date = s.inventory_date ;",
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
            