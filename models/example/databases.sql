{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.prolicious_SWIGGY_SALES_FACT_ITEMS_INTERMEDIATE AS SELECT distinct concat(Store_id,\'-\',fi.item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, DATE(TIMESTAMP(ordered_Date)) AS order_date, upper(fi.city) as CITY, VARIANT, CAST(base_mrp as float64)*CAST(UNITS_SOLD as float64) as MRP, STORE_ID, AREA_NAME, fi.ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, UPPER(cast(fi.L2_category as string)) AS product_category, UPPER(cast(fi.L2_category as string)) AS product_sub_category FROM MapleMonk.Swiggy_Prolicious_Instamart_sales fi ; CREATE OR REPLACE TABLE MAPLEMONK.prolicious_SWIGGY_SALES_FACT_ITEMS AS SELECT fi.order_id, fi.selling_price, fi.order_date, upper(coalesce(qc.city_name,fi.city)) as city, qc.state_name, fi.variant, fi.mrp, fi.store_id, fi.area_name, fi.item_code, fi.quantity, COALESCE(UPPER(cast(s.CATEGORY as string)), UPPER(cast(fi.product_category as string))) AS product_category, COALESCE(UPPER(cast(NULL as string)),UPPER(cast(fi.product_sub_category as string))) AS product_sub_category, s.master_sku as commonsku, upper(s.PRODUCT_NAME) as PRODUCT_NAME, upper(trim(product)) as product, upper(trim(pack_type)) as pack_type, flavour,subcat_1, subcat_2, sku_type, Cat FROM MapleMonk.prolicious_SWIGGY_SALES_FACT_ITEMS_INTERMEDIATE fi LEFT JOIN (SELECT swiggy_sku, master_sku, product_name, category, product, pack_type, flavour,subcat_1, subcat_2, sku_type, Cat from maplemonk.final_LATEST_SKU_MASTER where swiggy_sku is not null qualify row_number() over (partition by swiggy_sku order by swiggy_sku) = 1 ) s on upper(s.swiggy_sku) = upper(REPLACE(fi.ITEM_CODE, \'\"\', \'\')) LEFT JOIN (select upper(trim(city)) city_name, upper(trim(Quick_Commerce_City)) as Quick_Commerce_City, upper(trim(state)) state_name, from maplemonk.gs_qc_location_mapping qualify row_number() over (partition by upper(trim(Quick_Commerce_City)) order by 1 desc)=1 ) qc on qc.city_name = upper(fi.CITY) ;",
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
            