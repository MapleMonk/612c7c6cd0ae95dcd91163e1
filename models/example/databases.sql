{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table `ellementry-498507.maplemonk.final_SKU_MASTER` as select upper(cast(trim(PRIMARYKEY) as string)) as Master_SKU, upper(cast(PRODUCT_TITLE as string)) as Product_name, upper(trim(cast(STYLE as string))) as Style, upper(trim(cast(Category as string))) as Product_Category, upper(trim(cast(Sub_Category as string))) as Product_Sub_Category, upper(trim(cast(NATURE as string))) as Nature, upper(trim(cast(MRP as string))) as MRP, upper(trim(cast(ASN as string))) as Amazon_Sku, upper(trim(cast(FLIPKART_FSN as string))) as Flipkart_Sku, upper(trim(cast(MYNTRA_SKU_Code as string))) as Myntra_Sku, upper(trim(cast(Nykaa as string))) as Nykaa_Sku from maplemonk.Ellementry_SKU_MASTER g ;",
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
            