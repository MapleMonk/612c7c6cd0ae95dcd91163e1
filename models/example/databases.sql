{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.KA_Flipkart_Traffic_Fact_Items as select parse_date(\'%Y-%m-%d\',Impression_Date) Date ,\'FLIPKART\' Marketplace ,SKU_Id ,Listing_Id Product_ID ,p.skucode commonsku ,upper(coalesce(p.name, fi.Product_Title)) as PRODUCT_NAME_FINAL ,Upper(coalesce(p.category, fi.Category)) Product_Category ,p.sub_category AS PRODUCT_SUB_CATEGORY ,cast(Product_Clicks as INT64) Clicks ,cast(Product_Views as INT64) Impressions ,cast(Sales as FLOAT64) Sales ,cast(Conversion_Rate as float64)/100 Conversion_Rate ,cast(Product_Clicks as INT64)*cast(Conversion_Rate as float64)/100 Conversions from `MapleMonk.KA_S3_Flipkart_Daily_SKU_Traffic_Detailed` fi left join (select commonsku skucode, name, category, category_code sub_category, category_code, commonsku, TAX_RATE from maplemonk.final_sku_master qualify row_number()over (partition by commonsku order by 1) = 1 ) p on lower(fi.SKU_Id) = lower(p.skucode);",
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
            