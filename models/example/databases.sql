{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE philips_db.MAPLEMONK.PHILIPS_BLINKIT_FACT_ITEMS AS SELECT DISTINCT CONCAT( a.MRP, \'-\', ITEM_ID, CITY_NAME, \'-\', CITY_ID, \'-\', \'-\', CAST(QTY_SOLD AS FLOAT), DATE ) AS ORDER_ID, TO_DATE(DATE) AS ORDER_DATE, replace(c.MRP,\',\',\'\')::float AS MRP, CITY_NAME AS CITY, ITEM_ID AS PRODUCT_ID, CAST(REPLACE(QTY_SOLD, \'.0\', \'\') AS INTEGER) AS QUANTITY, replace(c.MRP,\',\',\'\')::float*CAST(REPLACE(QTY_SOLD, \'.0\', \'\') as integer) selling_price, upper(b.product_name) AS PRODUCT_NAME, UPPER(b.CATEGORY) AS PRODUCT_CATEGORY, upper(b.sub_Category) as product_sub_category, b.primarykey sku FROM (select * from MAPLEMONK.PHILIPS_BLINKIT_SALES_PARTNER_BIZ where TO_DATE(date) > (select max(date) from philips_db.MAPLEMONK.quick_commerce_historical_sales where lower(platform) = \'blinkit\') ) a left join (select distinct \"BLINKIT ID\", primarykey, category, sub_category, \"PRODUCT TITLE\" product_name from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MASTER) b on a.item_id = b.\"BLINKIT ID\" left join ( select * from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MRP_COGS where lower(marketplace) = \'blinkit\' ) c on upper(trim(b.primarykey)) = upper(trim(c.sku_code)) AND DATE(TO_DATE(a.date)) BETWEEN c.start_date AND c.end_date union all select null as ordeR_id, date as order_date, mcsi as mrp, upper(city) as city, sku_item_id as item_code, qty::float as quantity, value::float as selling_price, upper(item_description) as product_name, upper(bu) as product_category, upper(sag) as product_sub_Category, ctn as sku FROM philips_db.MAPLEMONK.quick_commerce_historical_sales where lower(platform) = \'blinkit\' ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PHILIPS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            