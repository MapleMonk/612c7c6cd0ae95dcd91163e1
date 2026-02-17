{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Sirona_Blinkit_Fact_items AS SELECT distinct concat(bs.mrp,\'-\',item_id,City_name,\'-\',city_id,\'-\',\'-\',CAST(qty_sold as float64),date) as order_id, \'BLINKIT\' as portal, cast(date as date) order_date, cast(bs.mrp as float64) mrp, city_name as city, item_id as product_id, cast(replace(qty_sold,\'.0\',\'\') as int64) quantity, coalesce(mp.name,bs.item_name) as product_name, coalesce(mp.Type,bs.category) as product_category, mp.Sub_Category as product_sub_category, cast(bs.mrp as float64) As Selling_price, cast(mp.mrp as float64) * CAST(qty_sold as float64) As mrp_sales, mp.Sirona_SKU_Code as sku from `Maplemonk.Sirona_blinkit_sales_partner_biz` bs LEFT JOIN ( select MRP, GST_, Name, Type, Portal, Portal_Code, Sub_Category, Sirona_SKU_Code from maplemonk.sirona_db_google_sheet_MP_Master qualify row_number() over(partition by portal, Portal_Code,Sirona_SKU_Code order by 1)=1 ) mp on UPPER(bs.item_id) = UPPER(mp.portal_code) AND UPPER(mp.Portal) = \'BLINKIT\'",
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
            