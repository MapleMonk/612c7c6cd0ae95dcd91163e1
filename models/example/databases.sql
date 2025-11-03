{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table saadaa-wh.maplemonk.saadaa_zepto_fact_items_intermediate as select EAN, concat(EAN,sku_number,city,date,selling_price) as order_item, CAST(MRP AS FLOAT64) AS MRP, CITY, CAST(Date AS timestamp) as Order_Date, SKU_NAME AS Product_name, Brand_Name as Brand, sku_number, sku_category, cast(selling_price as float64) as selling_price, manufacturer_id, SKU_Sub_Category as sub_category, cast(Gross_Selling_Value as float64) as Gross_Selling_Value, cast(Sales__Qty____Units as int64) as quantity, cast(Gross_Merchandise_Value as float64) as Gross_Merchandise_Value from saadaa-wh.`MAPLEMONK.Saadaa_design_sales` ; create or replace table saadaa-wh.maplemonk.saadaa_zepto_fact_items as select z.*, zp.pincode, p.state as state, m.easyecom_master_sku as commonsku, m.product_code, m.product_name as product_name_final, e.category as final_product_category, e.sub_category as final_sub_category, e.product_type, e.Gender, e.Color_Family, e.Product_Type as Product_Type_final, e.Product_Variant, e.FitType, e.Sleeve_Type, e.Neck_Collar_Type, e.size, e.CategoryType, e.Garment_Length_Type as garment_length from saadaa-wh.maplemonk.saadaa_zepto_fact_items_intermediate z left join saadaa-wh.maplemonk.saadaa_zepto_mapping m on lower(z.sku_number) = lower(m.sku_id) left join (select * from (select replace(commonsku,\' \',\'\') skucode, product_name name, category, cast(sub_category as string) as sub_category, cast(Gender as string) as Gender, CategoryType, Garment_Length_Type, Color_Family, Product_Type, Product_Variant, FitType, Sleeve_Type, Neck_Collar_Type, commonsku, size, row_number()over (partition by replace(commonsku,\' \',\'\') order by length(ifnull(replace(commonsku,\' \',\'\'),\'\')) desc) rw from maplemonk.saadaa_final_sku_master ) where rw = 1 ) e on lower(m.easyecom_master_sku) = lower(e.skucode) left join (select * from saadaa-wh.maplemonk.saadaa_design_pincode_zone_mapping qualify row_number() over (partition by city order by 1)=1 ) p on replace(p.city,\' \',\'\') = z.city left join maplemonk.GS_Zepto_Zity_Pincode zp on lower(replace(zp.city,\' \',\'\')) = lower(replace(z.city,\' \',\'\'));",
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
            