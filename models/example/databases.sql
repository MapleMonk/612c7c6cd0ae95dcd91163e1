{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PHILIPS_SWIGGY_SALES_FACT_ITEMS AS SELECT DISTINCT CONCAT( STORE_ID, \'-\', ITEM_CODE, AREA_NAME, \'-\', UPPER(CITY), \'-\', VARIANT, \'-\', CAST(UNITS_SOLD AS FLOAT), GMV, ORDERED_DATE ) AS ORDER_ID, TO_DATE(LEFT(ORDERED_DATE, 10)) AS ORDER_DATE, replace(c.MRP,\',\',\'\')::float*(a.units_sold::float) AS SELLING_PRICE, UPPER(CITY) AS CITY, UPPER(BRAND) AS BRAND, VARIANT, CAST(replace(c.MRP,\',\',\'\') AS FLOAT) AS MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD AS FLOAT) AS QUANTITY, UPPER(b.category) AS PRODUCT_CATEGORY, UPPER(b.sub_Category) AS PRODUCT_SUB_CATEGORY, UPPER(concat(b.PRODUCT_NAME, a.product_name)) AS PRODUCT_NAME FROM (select * from MAPLEMONK.PHILIPS_SWIGGY_SALES where TO_DATE(LEFT(ORDERED_DATE, 10)) > (select max(date) from philips_db.MAPLEMONK.quick_commerce_historical_sales where lower(platform) = \'swiggy\') ) a left join (select distinct \"SWIGGY ID\", primarykey, category, sub_category, \"PRODUCT TITLE\" product_name from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MASTER) b on a.item_code = b.\"SWIGGY ID\" left join ( select * from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MRP_COGS where lower(marketplace) = \'swiggy\' ) c on upper(trim(b.primarykey)) = upper(trim(c.sku_code)) AND DATE(TO_DATE(LEFT(a.ORDERED_DATE, 10))) BETWEEN c.start_date AND c.end_date union all select null as ordeR_id, date as order_date, value::float as selling_price, upper(city) as city, null as brand, null as variant, mcsi as mrp, null as store_id, null as area_name, sku_item_id as item_code, qty::float as quantity, upper(bu) as product_category, upper(sag) as product_sub_Category, upper(item_description) as product_name FROM philips_db.MAPLEMONK.quick_commerce_historical_sales where lower(platform) = \'swiggy\' ;",
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
            