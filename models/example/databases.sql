{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.KA_Flipkart_Traffic_Fact_Items as select parse_date(\'%Y-%m-%d\',Impression_Date) Date ,\'FLIPKART\' Marketplace ,SKU_Id ,Listing_Id Product_ID ,p.commonsku commonsku ,upper(coalesce(p.name, fi.Product_Title)) as PRODUCT_NAME_FINAL ,Product_Title ,Upper(coalesce(p.category, fi.Category)) Product_Category ,p.sub_category AS PRODUCT_SUB_CATEGORY ,cast(Product_Views as INT64) Impressions ,cast(Click_Through_Rate as FLOAT64)/100 CTR ,cast(Product_Clicks as INT64) Clicks ,cast(Sales as INT64) Conversions ,cast(Average_Selling_Price as FLOAT64) ASP ,cast(Revenue as FLOAT64) Sales ,cast(Conversion_Rate as float64)/100 Conversion_Rate from `MapleMonk.KA_S3_Flipkart_Daily_SKU_Traffic_Detailed` fi left join (select MARKETPLACE_SKU skucode, name, category, category_code sub_category, category_code, MRP, commonsku, TAX_RATE from (select * from maplemonk.final_sku_master where lower(data_source) like \'%flipkart%\') qualify row_number() over (partition by lower(ifnull(trim(MARKETPLACE_SKU),\'\')) order by lower(ifnull(trim(COMMONSKU),\'\')) desc) = 1 ) p on lower(trim(fi.SKU_Id)) = lower(p.skucode);",
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
            