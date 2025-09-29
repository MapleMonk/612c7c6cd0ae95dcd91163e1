{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Ritualistic_SWIGGY_ADS_FACT_ITEMS AS select \'SWIGGY\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(TOTAL_CTR as FLOAT64) as CTR ,COALESCE( SAFE.PARSE_DATE(\'%d-%b-%Y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%d-%m-%y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%m/%d/%Y\', TRIM(METRICS_DATE)) ) AS Date ,SAFE_CAST(0 as FLOAT64) as Views ,SAFE_CAST(TOTAL_CLICKS as FLOAT64) as Clicks ,Campaign_ID ,SAFE_CAST(TOTAL_IMPRESSIONS as FLOAT64) as Impressions ,Product_Name ,Campaign_Name ,SAFE_CAST(TOTAL_ROI as FLOAT64) * SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as ad_sales ,SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as spend ,\'Manual\' AS Type from MapleMonk.Ritualistic_SWIGGY_ADS ; CREATE OR REPLACE TABLE MAPLEMONK.Ritualistic_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, CAST(ordered_Date as date) as order_date, upper(city) as CITY, VARIANT, CAST(BASE_MRP as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, COALESCE(UPPER(cast(p.CATEGORY as string)), UPPER(cast(fi.L2_category as string))) AS product_category, COALESCE(UPPER(cast(p.sub_category as string)),UPPER(cast(fi.L2_category as string))) AS product_sub_category, p.commonsku, p.EAN, p.GST_Rate, p.New_HSN_from_17_Sept_25 as HSN, p.product_image_url, p.cogs, upper(PRODUCT_NAME) as PRODUCT_NAME, coalesce(upper(p.Product_name),replace(fi.PRODUCT_NAME,\'RITUALISTIC \',\'\')) AS product_name_final FROM MapleMonk.Ritualistic_SWIGGY_SALES fi left join (select * from (select EAN, commonsku, category, sub_category, GST_Rate, Product_name, New_HSN_from_17_Sept_25, Product_Launch_date, product_image_url, cogs, row_number()over (partition by commonsku order by length(ifnull(commonsku,\'\')) desc) rw from neshanka-wh.maplemonk.Neshanka_SKU_Master ) where rw = 1) p on lower(replace(fi.ITEM_CODE,\' \',\'\')) = lower(p.commonsku) ;",
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
            