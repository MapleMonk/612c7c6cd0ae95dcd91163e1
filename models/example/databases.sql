{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.FINAL_SKU_MASTER AS with lisitng as (select * from (select upper(replace(SKU_Code,\' \',\'\')) COMMONSKU ,upper(replace(Seller_SKU_on_Channel,\' \',\'\')) MARKETPLACE_SKU ,upper(Channel_Code) MARKETPLACE ,row_number() over (partition by Upper(Channel_Code), upper(Seller_SKU_on_Channel) order by 1 desc) rw from maplemonk.zouk_unicommerce_get_product_listing ) where rw = 1 ), COMMONSKU_MASTER as ( Select * from ( SELECT UPPER(Name) as NAME, UPPER(COLOR) as colour, UPPER(BRAND) as BRAND, UPPER(Category_Name) as CATEGORY, UPPER(Category_Code) as Category_Code, UPPER(COLOR) as print, NULL as GENDER, UPPER(collection) AS sub_category, UPPER(collection) as collection, UPPER(replace(Product_Code,\' \',\'\')) as commonsku_master, SAFE_CAST(mrp AS FLOAT64) as MRP, SAFE_CAST(Base_Price AS FLOAT64) as BAU_Offline, SAFE_CAST(Base_Price AS FLOAT64) as BAU_Online, Product_Bucket, Product_Type, SAFE_CAST(SPLIT(GST_Tax_Type_Code, \'_\')[OFFSET(0)] AS FLOAT64)/100 as TAX_RATE, UPPER(SIZE) as SIZE, SAFE_CAST(Weight__gms_ AS FLOAT64) as weight, SAFE_CAST(Length__mm_ AS FLOAT64) as length, SAFE_CAST(Width__mm_ AS FLOAT64) as width, (SAFE_CAST(Length__mm_ AS FLOAT64) * SAFE_CAST(Height__mm_ AS FLOAT64) * SAFE_CAST(Width__mm_ AS FLOAT64)) / 1000 as volume, HSN_CODE, ROW_NUMBER() OVER (PARTITION BY Product_Code ORDER BY 1) crw FROM maplemonk.unicommerce_zouk_get_product_master )where crw = 1 ) select coalesce(l.COMMONSKU,cm.commonsku_master) as COMMONSKU ,l.MARKETPLACE_SKU ,l.MARKETPLACE ,cm.* from lisitng l Full Outer join COMMONSKU_MASTER CM on upper(l.commonsku) = upper(cm.commonsku_master);",
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
            