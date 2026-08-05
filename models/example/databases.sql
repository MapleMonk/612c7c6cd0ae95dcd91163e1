{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; INSERT INTO snitch_db.maplemonk.inventory_daily_snapshot ( snapshot_ts, sku_group, category, cut_size, l1_category, original_price, current_slashed_price, online_inventory, offline_inventory, sku_class, shopify_sales, shopify_quantity, shopify_discount, offline_sales, offline_quantity, offline_discount, mp_sales, mp_quantity, mp_discount ) with prod as ( select sku_group, category, CASE when online_sku_class = \'cut\' then \'cut\' else \'non_cut\' END AS cut_size, l1_category, original_price, current_slashed_price, online_inventory, offline_inventory, ONLINE_SKU_CLASS as sku_class from snitch_db.maplemonk.base_product where status = \'ACTIVE\' and online_inventory>0 ), shopify_sales as ( select sku_group, sum(gross_sales) as sales, sum(gross_quantity) as quantity, sum(discount_amount) as discount from snitch_db.maplemonk.horizontal_sales_categories where type = \'Shopify\' and date = current_date -1 group by 1 ), offline_sales as ( select sku_group, sum(gross_sales) as sales, sum(gross_quantity) as quantity, sum(discount_amount) as discount from snitch_db.maplemonk.horizontal_sales_categories where type = \'Store\' and date = current_date -1 group by 1 ), mp_sales as ( select sku_group, sum(gross_sales) as sales, sum(gross_quantity) as quantity, sum(discount_amount) as discount from snitch_db.maplemonk.horizontal_sales_categories where type = \'Marketplace\' and date = current_date -1 group by 1 ) select CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()), a.*, ifnull(b.sales,0)::int as shopify_sales, ifnull(b.quantity,0)::int as shopify_quantity, ifnull(b.discount,0)::int as shopify_discount, ifnull(c.sales,0)::int as offline_sales, ifnull(c.quantity,0)::int as offline_quantity, ifnull(c.discount,0)::int as offline_discount, ifnull(d.sales,0)::int as mp_sales, ifnull(d.quantity,0)::int as mp_quantity, ifnull(d.discount,0)::int as mp_discount from prod a left join shopify_sales b on a.sku_group = b.sku_group left join offline_sales c on a.sku_group = c.sku_group left join mp_sales d on a.sku_group = d.sku_group ; CREATE OR REPLACE table snitch_db.maplemonk.inventory_daily_snapshot_latest AS WITH ranked AS ( SELECT *, DATE(CONVERT_TIMEZONE(\'Asia/Kolkata\', snapshot_ts)) AS snapshot_date, ROW_NUMBER() OVER ( PARTITION BY DATE(CONVERT_TIMEZONE(\'Asia/Kolkata\', snapshot_ts)), sku_group ORDER BY snapshot_ts DESC ) AS rn FROM snitch_db.maplemonk.inventory_daily_snapshot ) SELECT * FROM ranked WHERE rn = 1;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            