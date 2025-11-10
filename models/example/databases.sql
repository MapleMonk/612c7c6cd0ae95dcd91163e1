{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Ritualistic_Blinkit_Fact_items AS SELECT distinct concat(mrp,\'-\',item_id,City_name,\'-\',city_id,\'-\',\'-\',CAST(qty_sold as float64),date) as order_id, cast(date as date) order_date, cast(mrp as float64) mrp, city_name as city, item_id as product_id, cast(replace(qty_sold,\'.0\',\'\') as int64) quantity, item_name as product_name, p.commonsku, coalesce(p.category,bs.category) as product_category, p.sub_category, p.GST_Rate, p.EAN, p.Product_name Product_name_final, p.New_HSN_from_17_Sept_25, p.Product_Launch_date, p.product_image_url, p.cogs, (case when CAST(Date as date) < \'2025-09-22\' then cast(SP_till_21_Sep as float64) else cast(SP_from_22_Sep as float64) end) * (cast(replace(qty_sold,\'.0\',\'\') as int64)) as Selling_Price_final FROM `maplemonk.Blinkit_Ritualistic_sales_partner_biz` bs left join (select * from maplemonk.gs_qc_sku_mapping where Blinkit_item_id <> \'-\' qualify row_number() over (partition by Blinkit_item_id order by 1) =1 ) qc on qc.Blinkit_item_id = bs.item_id left join (select * from (select EAN, commonsku, category, sub_category, GST_Rate, Product_name, New_HSN_from_17_Sept_25, Product_Launch_date, product_image_url, cogs, row_number()over (partition by ean order by length(ifnull(ean,\'\')) desc) rw from neshanka-wh.maplemonk.Neshanka_SKU_Master ) where rw = 1) p on lower(qc.ean) = lower(p.ean);",
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
            