{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.final_SKU_MASTER as - select upper(cast(trim(PRIMARYKEY) as string)) as Master_SKU, upper(cast(PRODUCT_TITLE as string)) as Product_name, upper(trim(cast(BRAND as string))) as Brand, upper(trim(cast(STYLE as string))) as Style, upper(trim(cast(Category as string))) as Product_Category, upper(trim(cast(Sub_Category as string))) as Product_Sub_Category, upper(trim(cast(NATURE as string))) as Nature, upper(trim(cast(ASN as string))) as Amazon_Sku, upper(trim(cast(Nykaa as string))) as Nykaa_Sku, upper(trim(cast(Swiggy as string))) as Swiggy_Sku, upper(trim(cast(FLIPKART_FSN as string))) as Flipkart_Sku from maplemonk.GS_Ozone_SKU_MASTER g ;",
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
            