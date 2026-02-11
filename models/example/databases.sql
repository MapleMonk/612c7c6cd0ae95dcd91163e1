{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Carlton_London_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, CAST(ordered_Date as date) as order_date, upper(city) as CITY, VARIANT, CAST(BASE_MRP as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE as product_id, CAST(UNITS_SOLD as float64) as quantity, COALESCE(UPPER(cast(sm.category as string)), UPPER(cast(fi.L2_category as string))) AS product_category, COALESCE(UPPER(cast(null as string)),UPPER(cast(fi.L2_category as string))) AS product_sub_category, sm.master_sku as commonsku, upper(fi.PRODUCT_NAME) as PRODUCT_NAME, sm.product_name as product_name_final, FROM MapleMonk.Instamart_Carlton_London_SALES fi LEFT JOIN (SELECT swiggy_sku, master_sku, product_name, category from maplemonk.carlton_london_sku_master qualify row_number() over (partition by swiggy_sku order by master_sku) = 1 ) sm on upper(sm.swiggy_sku) = upper(REPLACE(fi.item_code, \'\"\', \'\')) ;",
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
            