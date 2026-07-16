{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.ozone_SWIGGY_ADS_FACT_ITEMS AS select \'SWIGGY\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(TOTAL_CTR as FLOAT64) as CTR ,COALESCE( SAFE.PARSE_DATE(\'%d-%b-%Y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%d-%m-%y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%m/%d/%Y\', TRIM(METRICS_DATE)) ) AS Date ,SAFE_CAST(0 as FLOAT64) as Views ,SAFE_CAST(TOTAL_CLICKS as FLOAT64) as Clicks ,Campaign_ID ,SAFE_CAST(TOTAL_IMPRESSIONS as FLOAT64) as Impressions ,Product_Name ,Campaign_Name ,SAFE_CAST(TOTAL_ROI as FLOAT64) * SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as ad_sales ,SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as spend ,\'Manual\' AS Type from MapleMonk.Swiggy_Instamart_Ozone_ads; CREATE OR REPLACE TABLE MAPLEMONK.ozone_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, DATE(TIMESTAMP(ordered_Date)) AS order_date, upper(city) as CITY, VARIANT, CAST(base_mrp as float64)*CAST(UNITS_SOLD as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, COALESCE(UPPER(cast(p.Product_category as string)), UPPER(cast(fi.L2_category as string))) AS product_category, COALESCE(UPPER(cast(p.Product_sub_category as string)),UPPER(cast(fi.L2_category as string))) AS product_sub_category, upper(trim(p.master_sku)) as commonsku, upper(fi.PRODUCT_NAME) as PRODUCT_NAME, upper(p.PRODUCT_NAME) as PRODUCT_NAME_final, CAST(gmv as float64) as Selling_Price_final, upper(replace(delivery_state,\' Division\',\'\')) as State FROM MapleMonk.Swiggy_Instamart_Ozone_sales fi left join (select swiggy_sku, Master_sku, Product_name, Brand, Style, Product_category, Product_sub_category, Nature from maplemonk.final_SKU_MASTER where swiggy_sku is not null qualify row_number() over (partition by swiggy_sku order by master_sku) = 1 ) p on lower(fi.item_code) = lower(trim(p.swiggy_sku)) left join (select * from maplemonk.gs_state_city_mapping qualify row_number() over (partition by delivery_city order by 1)=1 ) pin on trim(upper(pin.delivery_city)) = upper(fi.city) ;",
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
            