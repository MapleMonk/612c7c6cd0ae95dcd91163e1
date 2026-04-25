{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.prolicious_SWIGGY_ADS_FACT_ITEMS AS select \'SWIGGY\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(TOTAL_CTR as FLOAT64) as CTR ,COALESCE( SAFE.PARSE_DATE(\'%d-%b-%Y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%d-%m-%y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%m/%d/%Y\', TRIM(METRICS_DATE)) ) AS Date ,SAFE_CAST(0 as FLOAT64) as Views ,SAFE_CAST(TOTAL_CLICKS as FLOAT64) as Clicks ,Campaign_ID ,SAFE_CAST(TOTAL_IMPRESSIONS as FLOAT64) as Impressions ,Product_Name ,Campaign_Name ,SAFE_CAST(TOTAL_ROI as FLOAT64) * SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as ad_sales ,SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as spend ,\'Manual\' AS Type from MapleMonk.Prolicious_Swiggy_ads; CREATE TABLE IF NOT EXISTS maplemonk.sku_swiggy_mapping( EAN STRING, commonsku STRING, GST_Rate FLOAT64, HSN STRING, Product_Launch_date DATE, product_image_url STRING, cogs FLOAT64 ) ; Create Table If Not Exists maplemonk.swiggy_qc_sku_mapping( EAN String, SKU String, Swiggy_item_id String, _airbyte_ab_id String, _airbyte_emitted_at Timestamp, _airbyte_normalized_at Timestamp, _airbyte_swiggy_qc_sku_mapping_hashid String ) ; CREATE OR REPLACE TABLE MAPLEMONK.prolicious_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',fi.item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, DATE(TIMESTAMP(ordered_Date)) AS order_date, upper(city) as CITY, VARIANT, CAST(base_mrp as float64)*CAST(UNITS_SOLD as float64) as MRP, STORE_ID, AREA_NAME, fi.ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, COALESCE(UPPER(cast(s.CATEGORY as string)), UPPER(cast(fi.L2_category as string))) AS product_category, COALESCE(UPPER(cast(s.sub_category as string)),UPPER(cast(fi.L2_category as string))) AS product_sub_category, s.master_sku as commonsku, upper(fi.PRODUCT_NAME) as PRODUCT_NAME, CAST(base_mrp as float64)*CAST(UNITS_SOLD as float64) as Selling_Price_final FROM MapleMonk.Prolicious_Swiggy_sales fi LEFT JOIN (SELECT swiggy_sku, master_sku, product_name, category, sub_category, style, nature from maplemonk.final_SKU_MASTER qualify row_number() over (partition by swiggy_sku order by master_sku) = 1 ) s on upper(s.swiggy_sku) = upper(REPLACE(fi.ITEM_CODE, \'\"\', \'\')) ;",
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
            