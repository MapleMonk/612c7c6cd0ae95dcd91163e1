{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Inventory_planning_summary_snitch as select a.sku, case when e.status is NULL then \'Not in Shopify\' else e.status end as status, case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end sku_group, case when category = \'Underpants\' then \'Boxers + Innerwear + Pyjamas\' when category = \'Sweatshirt\' then \'Sweaters\' when category = \'Cargo\' then \'BottomWear\' when category = \'Denim\' then \'Denim\' when category = \'Jacket\' then \'Jackets\' when category = \'T-Shirt\' then \'T-Shirts\' when category = \'Shirt\' then \'Shirts\' when category = \'Jogger\' then \'Joggers\' when category = \'Pyjama\' then \'Boxers + Innerwear + Pyjamas\' when category = \'Perfumes\' then \'Perfume & Cologne\' when category = \'Shorts\' then \'Shorts\' when category = \'Jogsuit\' then \'Joggers\' when category = \'Accessories\' then \'Accessories\' when category = \'Sweater\' then \'Sweaters\' when category = \'Trouser\' then \'BottomWear\' when category = \'Co-Ords\' then \'Co-Ords\' when category = \'Chino\' then \'BottomWear\' when category = \'Boxer\' then \'Boxers + Innerwear + Pyjamas\' end as category_mapped, product_name_shopify, a.units_on_hand, a.units_on_hand*b.mrp Inventory_on_hand, c.revenue, c.quantity*d.latest_cost COGS, f.latest_inward_Date, f.warehouse_name, g.first_ordeR_date, a.facility, case when g.first_ordeR_date is null then \'New\' when g.first_ordeR_date >= f.latest_inward_Date then \'New\' when g.first_ordeR_date < f.latest_inward_Date then \'Repeat\' end as new_inward_flag, sum(case when a.units_on_hand >=10 then 1 else 0 end) over (partition by case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end ) sku_inventory_greater_than_10_flag from (select \"Item Type skuCode\" sku, category, \"Item Type Name\" product_name, facility,count(distinct \"Item Code\") Units_on_hand from snitch_db.maplemonk.unicommerce_inventory_aging group by 1,2,3,4 order by 5 desc) a left join ( select sku, MRP, product_name_shopify from ( select sku, MRP, product_name_shopify,row_number() over (partition by sku order by order_date desc) rw from snitch_db.maplemonk.unicommerce_fact_items_snitch) where rw=1) b on a.sku = b.sku full outer join ( select sku, sum(selling_price) revenue, sum(subordeR_quantity) quantity, sum(suborder_quantity*mrp) mrp_sales from snitch_db.maplemonk.unicommerce_fact_items_snitch where order_date::date >= getdate()::date-30 group by 1 )c on a.sku = c.sku left join ( select sku, latest_cost from (select \"Item Type skuCode\" sku, \"Unit price with tax\" latest_cost, row_number() over (partition by \"Item Type skuCode\" order by left(\"Item Created On\",10)::date desc) rw from snitch_db.maplemonk.unicommerce_inventory_aging ) where rw = 1 )d on a.sku = d.sku left join ( select sku, status from( select b.value:\"sku\" as SKU, status, row_number() over (partition by b.value:\"sku\" order by updated_at desc) rw from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS, lateral flatten (INPUT => variants)b ) where rw = 1 ) e on lower(a.sku)=lower(e.sku) left join ( select sku_group, date as latest_inward_Date,warehouse_name from ( select *, row_number() over (partition by sku_group order by quantity desc) rw from( select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group ,left(\"CREATED\",10)::date date ,case when _AB_SOURCE_FILE_URL like \'%SAPL-SR%\' then \'SAPL-SR\' when _AB_SOURCE_FILE_URL like \'%SAPL-EMIZA%\' then \'SAPL-EMIZA\' when _AB_SOURCE_FILE_URL like \'%SAPL-WH%\' then \'SAPL-WH\' end warehouse_name ,sum(\"Putaway Quantity\") quantity from snitch_db.MAPLEMONK.UNICOMMERCE_PUTAWAY_REPORT where type = \'PUTAWAY_GRN_ITEM\' group by 1,2,3 )) where rw = 1 ) f on case when right(a.sku,2) = \'-S\' then left(a.sku,len(a.sku)-2) else replace(a.sku,concat(\'-\',split_part(a.sku,\'-\',-1)),\'\') end = f.sku_group left join ( select sku, min(ordeR_Date::Date) first_order_date from snitch_db.maplemonk.unicommerce_fact_items_snitch group by 1 )g on a.sku = g.sku order by units_on_hand desc ; create or replace table snitch_db.maplemonk.inventory_aging_buckets_snitch as select a.* ,b.inventory atp_units from (select \"Item Type skuCode\" sku ,REVERSE(SUBSTRING(REVERSE(replace(\"Item Type skuCode\",\'\"\',\'\')), CHARINDEX(\'-\', REVERSE(replace(\"Item Type skuCode\",\'\"\',\'\'))) + 1)) sku_group ,\"Item Type Name\" producT_name ,case when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=0 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=30 then \'0-30\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=31 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=60 then \'31-60\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=61 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=90 then \'61-90\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=91 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=120 then \'91-120\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=121 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=150 then \'121-150\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=151 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=180 then \'151-180\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=181 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=210 then \'181-210\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=211 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=240 then \'211-240\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=241 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=270 then \'241-270\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=271 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=300 then \'271-300\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=301 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=330 then \'301-330\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=331 and datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) <=360 then \'331-360\' when datediff(day,coalesce(n.\"Item Created On\",left(m.\"Item Created On\",10)::date),getdate()::date) >=361 then \'>360\' end as days_in_warehouse ,count(distinct m.\"Item Code\") Units_on_hand ,category ,case when size = \'28\' then \'XS\' when size = \'30\' then \'S\' when size = \'32\' then \'M\' when size = \'34\' then \'L\' when size = \'36\' then \'XL\' when size = \'38\' then \'XXL\' when size = \'40\' then \'3XL\' when size = \'42\' then \'4XL\' when size = \'44\' then \'5XL\' when size = \'46\' then \'6XL\' else size end as size_mapped ,size from (select * from snitch_db.maplemonk.unicommerce_inventory_aging where CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',_airbyte_emitted_at :: DATETIME)::date = current_date() ) m left join (select \"Item Code\", try_to_Date(left(\"Item Created On\",10),\'dd-mm-yyyy\') \"Item Created On\" from snitch_db.maplemonk.old_inventory_yelahanka ) n on m.\"Item Code\" = n.\"Item Code\" group by 1,2,3,4,6,7,8 ) a left join ( select skucode, sum(inventory) inventory from snitch_db.maplemonk.snitch_unicommerce_get_inventory_snapshot where facility <> \'4mclothingllp\' group by 1 ) b on a.sku = b.skucode ; ; Create or replace table snitch_db.maplemonk.bad_inventory_snitch as select coalesce(a.sku_group,b.sku_group) sku_group, least(ifnull(a.date,\'2070-01-01\'), ifnull(b.date,\'2070-01-01\')) date, ifnull(a.quantity,0) + ifnull(b.quantity,0) quantity from (select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group ,left(\"Item Created On\",10)::date date ,count(distinct \"Item Code\") quantity from snitch_db.maplemonk.unicommerce_itembarcode_report where \"Inventory type\" = \'BAD_INVENTORY\' group by 1,2 )a full outer join (select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group ,to_date(left(\"Item Created On\",10),\'DD-MM-YYYY\') date ,count(distinct \"Item Code\") quantity from snitch_db.maplemonk.old_inventory_yelahanka where \"Inventory type\" = \'BAD_INVENTORY\' group by 1,2) b on a.sku_group = b.sku_group ;",
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
                        