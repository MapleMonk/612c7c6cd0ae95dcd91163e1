{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PHILIPS_ZEPTO_FACT_ITEMS AS SELECT EAN, CONCAT(EAN, \"SKU Number\", CITY, DATE) AS ORDER_ITEM, replace(c.MRP,\',\',\'\')::float AS MRP, CITY, CAST(DATE AS TIMESTAMP) AS ORDER_DATE, \"Brand Name\" AS BRAND, \"SKU Number\" SKU_NUMBER, b.primarykey as sku_id, CAST(\"Sales (Qty) - Units\" AS INTEGER) AS QUANTITY, replace(c.MRP,\',\',\'\')::float*CAST(\"Sales (Qty) - Units\" AS INTEGER) AS SELLING_PRICE, upper(b.product_name) AS PRODUCT_NAME, UPPER(b.CATEGORY) AS PRODUCT_CATEGORY, upper(b.sub_Category) as product_sub_category FROM (select * from philips_db.MAPLEMONK.ZEPTO_PHILIPS_SALES_SALES where TO_DATE(date) > (select max(date) from philips_db.MAPLEMONK.quick_commerce_historical_sales where lower(platform) = \'zepto\') ) a left join (select distinct \"ZEPTO ID\", primarykey, category, sub_category, \"PRODUCT TITLE\" product_name from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MASTER) b on a.\"SKU Number\" = b.\"ZEPTO ID\" left join ( select * from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MRP_COGS where lower(marketplace) = \'zepto\' ) c on upper(trim(b.primarykey)) = upper(trim(c.sku_code)) AND DATE(TO_DATE(a.date)) BETWEEN c.start_date AND c.end_date union all select null as ean, sku_item_id as order_item, mcsi as mrp, upper(city) as city, date as order_date, null as brand, sku_item_id as sku_number, ctn sku_id, qty::float as quantity, value::float as selling_price, upper(item_description) as product_name, upper(bu) as product_category, upper(sag) as product_sub_Category FROM philips_db.MAPLEMONK.quick_commerce_historical_sales where lower(platform) = \'zepto\' ;",
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
            