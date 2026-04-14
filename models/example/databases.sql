{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.babygo_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',fi.item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, DATE(TIMESTAMP(ordered_Date)) AS order_date, upper(city) as CITY, VARIANT, CAST(base_mrp as float64)*CAST(UNITS_SOLD as float64) as MRP, STORE_ID, AREA_NAME, fi.ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, COALESCE(UPPER(cast(p.CATEGORY as string)), UPPER(cast(fi.L2_category as string))) AS product_category, COALESCE(UPPER(cast(p.sub_category as string)),UPPER(cast(fi.L2_category as string))) AS product_sub_category, p.sku_number commonsku, p.style_id, p.lot, upper(fi.PRODUCT_NAME) as PRODUCT_NAME, CAST(base_mrp as float64)*CAST(UNITS_SOLD as float64) as Selling_Price_final FROM MapleMonk.BabyGo_Swiggy_sales fi left join (select item_code, sku_number, lot, style_id, category, cast(null as string) sub_category from maplemonk.gs_qc_sku_mapping where platform = \'SWIGGY\' qualify row_number() over (partition by item_code order by sku_number) = 1 ) p on fi.item_code = p.item_code ;",
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
            