{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Atovio_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS GMV, CAST(left(ordered_Date,10) as date) as order_date, upper(city) as CITY, VARIANT, CAST(BASE_MRP as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, UPPER(coalesce(p.category,cast(L2_category as string))) AS product_category, UPPER(coalesce(p.sub_category,(cast(L2_category as string)))) AS product_sub_category, UPPER(PRODUCT_NAME) as Product_Name, p.style, p.commonsku ,cast(p.instamart_Selling_price as float64) as selling_price ,round(instamart_Selling_price * (gs_tax/100),2) as gs_tax_amount ,cast(p.GS_Tax as int64) GS_Tax ,cast(mrp.mrp as float64)*CAST(UNITS_SOLD AS INT64)mrp_sales ,cast(mrp.msp as float64)*CAST(UNITS_SOLD AS INT64)msp_sales ,cast(mrp.cogs as float64)*CAST(UNITS_SOLD AS INT64) total_cogs ,cast(mrp.mrp as float64)*CAST(UNITS_SOLD AS INT64) - CAST(instamart_Selling_price AS FLOAT64) discount_mrp FROM maplemonk.sales_report_sales LEFT JOIN (select * from (select upper(trim(product_title)) as name, upper(trim(category)) category, upper(trim(sub_category)) sub_category, upper(trim(style)) style , upper(trim(primarykey)) as commonsku, trim(upper(instamart_id)) as instamart_id, cast(tax as int64) as GS_Tax, cast(Instamart_Selling_price as int64) instamart_Selling_price, row_number()over (partition by trim(upper(instamart_id)) order by length(ifnull(trim(upper(blinkit_id)),\'\')) desc) rw from Maplemonk.Atovio_mapping_SKU_MASTER where trim(upper(instamart_id)) is not null ) where rw = 1 ) p ON LOWER(CAST(ITEM_CODE AS STRING)) = lower(p.instamart_id) left join (select * from maplemonk.mapping_sku_mrp_cogs qualify row_number() over (partition by sku_code order by 1) = 1) mrp on p.commonsku = mrp.sku_code;",
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
            