{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Inventory_planning_summary_snitch as select a.sku, case when e.status is NULL then \'Not in Shopify\' else e.status end as status, case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end sku_group, case when category = \'Underpants\' then \'Boxers + Innerwear + Pyjamas\' when category = \'Sweatshirt\' then \'Sweaters\' when category = \'Cargo\' then \'BottomWear\' when category = \'Denim\' then \'Denim\' when category = \'Jacket\' then \'Jackets\' when category = \'T-Shirt\' then \'T-Shirts\' when category = \'Shirt\' then \'Shirts\' when category = \'Jogger\' then \'Joggers\' when category = \'Pyjama\' then \'Boxers + Innerwear + Pyjamas\' when category = \'Perfumes\' then \'Perfume & Cologne\' when category = \'Shorts\' then \'Shorts\' when category = \'Jogsuit\' then \'Joggers\' when category = \'Accessories\' then \'Accessories\' when category = \'Sweater\' then \'Sweaters\' when category = \'Trouser\' then \'BottomWear\' when category = \'Co-Ords\' then \'Co-Ords\' when category = \'Chino\' then \'BottomWear\' when category = \'Boxer\' then \'Boxers + Innerwear + Pyjamas\' end as category_mapped, a.units_on_hand, a.units_on_hand*b.mrp Inventory_on_hand, c.revenue, c.quantity*d.latest_cost COGS, f.latest_inward_Date, sum(case when a.units_on_hand >=10 then 1 else 0 end) over (partition by case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end ) sku_inventory_greater_than_10_flag from (select \"Item Type skuCode\" sku, category, count(distinct \"Item Code\") Units_on_hand from snitch_db.maplemonk.unicommerce_inventory_aging group by 1,2 order by 3 desc) a left join ( select sku, MRP from ( select sku, MRP, row_number() over (partition by sku order by order_date desc) rw from snitch_db.maplemonk.unicommerce_fact_items_snitch) where rw=1) b on a.sku = b.sku full outer join ( select sku, sum(selling_price) revenue, sum(subordeR_quantity) quantity, sum(suborder_quantity*mrp) mrp_sales from snitch_db.maplemonk.unicommerce_fact_items_snitch where order_date::date >= getdate()::date-30 group by 1 )c on a.sku = c.sku left join ( select sku, latest_cost from (select \"Item Type skuCode\" sku, \"Unit price with tax\" latest_cost, row_number() over (partition by \"Item Type skuCode\" order by left(\"Item Created On\",10)::date desc) rw from snitch_db.maplemonk.unicommerce_inventory_aging ) where rw = 1 )d on a.sku = d.sku left join (select distinct b.value:\"sku\" as SKU, status from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS, lateral flatten (INPUT => variants)b) e on a.sku=e.sku left join ( select sku_group, date as latest_inward_Date from ( select *, row_number() over (partition by sku_group order by quantity desc) rw from( select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group ,left(\"Item Created On\",10)::date date ,count(distinct \"Item Code\") quantity from snitch_db.maplemonk.unicommerce_itembarcode_report where \"Inventory type\" = \'GOOD_INVENTORY\' group by 1,2 ) where quantity > 200) where rw = 1 ) f on case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end = f.sku_group order by units_on_hand desc ; create or replace table snitch_db.maplemonk.inventory_aging_buckets_snitch as select a.sku, case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end sku_group, b.title product_name, days_in_warehouse, units_on_hand, category, case when size = \'28\' then \'XS\' when size = \'30\' then \'S\' when size = \'32\' then \'M\' when size = \'34\' then \'L\' when size = \'36\' then \'XL\' when size = \'38\' then \'XXL\' when size = \'40\' then \'3XL\' when size = \'42\' then \'4XL\' when size = \'44\' then \'5XL\' when size = \'46\' then \'6XL\' else size end as size_mapped, size, sorting_number, d.sku_category, d.buckets, d.cumulative_share from (select \"Item Type skuCode\" sku, category, size, case when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=0 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=30 then \'0-30\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=31 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=90 then \'31-90\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=91 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=180 then \'91-180\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=181 then \'>180\' end as days_in_warehouse, left(m.\"Item Created On\",10)::date, case when days_in_warehouse = \'0-30\' then 15 when days_in_warehouse = \'31-90\' then 75 when days_in_warehouse = \'91-180\' then 150 when days_in_warehouse = \'>180\' then 200 end as sorting_number, count(distinct m.\"Item Code\") Units_on_hand from snitch_db.maplemonk.unicommerce_inventory_aging m left join (select \"Item Code\", try_to_Date(left(\"Item Created On\",10),\'dd-mm-yyyy\') \"Item Created On\" from snitch_db.maplemonk.old_inventory_yelahanka ) n on m.\"Item Code\" = n.\"Item Code\" group by 1,2,3,4,5 order by 6 desc) a left join (select distinct a.title, replace(b.value:sku,\'\"\',\'\') sku from Snitch_db.maplemonk.Shopify_All_products A, lateral flatten (INPUT => variants) B) b on a.sku = b.sku left join ( select distinct sku_group, buckets, cumulative_share,\'Top-SKUs\' as sku_category from ( select m.*, avg_unit_sales_per_skugroup, avg_return_percent_per_skugroup, case when units > avg_unit_sales_per_skugroup and div0(return_units,units) < avg_return_percent_per_skugroup then \'High Sales Low Returns\' when units > avg_unit_sales_per_skugroup and div0(return_units,units) > avg_return_percent_per_skugroup then \'High Sales High Returns\' when units < avg_unit_sales_per_skugroup and div0(return_units,units) < avg_return_percent_per_skugroup then \'Low Sales Low Returns\' when units < avg_unit_sales_per_skugroup and div0(return_units,units) > avg_return_percent_per_skugroup then \'Low Sales High Returns\' end as buckets from ( select * from ( select sku_group, product_name, revenue, units, return_units,sum(share) over (order by share desc rows between unbounded preceding and current row) cumulative_share from( select sku_group, product_name, revenue, units, return_units, 100*div0(revenue,sum(revenue) over (partition by 1)) share from ( select case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end sku_group, b.title product_name, sum(selling_price) revenue, sum(suborder_quantity) units, sum(return_quantity) return_units from snitch_db.maplemonk.unicommerce_fact_items_snitch a left join (select distinct a.title, replace(b.value:sku,\'\"\',\'\') sku from Snitch_db.maplemonk.Shopify_All_products A, lateral flatten (INPUT => variants) B) b on a.sku = b.sku where order_status <> \'CANCELLED\' and lower(marketplace) = \'shopify\' and order_date >= getdate()::date - 180 and order_date < getdate()::date group by 1,2 order by 3 desc ) order by share desc) order by share desc ) where cumulative_share < 50 )m left join ( select div0(sum(units),count(distinct sku_group)) avg_unit_sales_per_skugroup, div0(sum(return_units),sum(units)) avg_return_percent_per_skugroup from ( select sku_group, product_name, revenue, units, return_units,sum(share) over (order by share desc rows between unbounded preceding and current row) cumulative_share from( select sku_group, product_name, revenue, units, return_units, 100*div0(revenue,sum(revenue) over (partition by 1)) share from ( select case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end sku_group, b.title product_name, sum(selling_price) revenue, sum(suborder_quantity) units, sum(return_quantity) return_units from snitch_db.maplemonk.unicommerce_fact_items_snitch a left join (select distinct a.title, replace(b.value:sku,\'\"\',\'\') sku from Snitch_db.maplemonk.Shopify_All_products A, lateral flatten (INPUT => variants) B) b on a.sku = b.sku where order_status <> \'CANCELLED\' and lower(marketplace) = \'shopify\' and order_date >= getdate()::date - 180 and order_date < getdate()::date group by 1,2 order by 3 desc ) order by share desc) order by share desc ) where cumulative_share < 50 )n order by cumulative_share asc ) )d on case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end = d.sku_group ; Create or replace table snitch_db.maplemonk.bad_inventory_snitch as select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group ,left(\"Item Created On\",10)::date date ,count(distinct \"Item Code\") quantity from snitch_db.maplemonk.unicommerce_itembarcode_report where \"Inventory type\" = \'BAD_INVENTORY\' group by 1,2",
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
                        