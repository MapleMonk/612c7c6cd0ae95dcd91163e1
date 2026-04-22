{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.final_SKU_MASTER as select upper(cast(trim(ASN) as string)) as amazon_sku, upper(cast(trim(FLIPKART_FSN) as string)) as flipkart_sku, upper(cast(trim(Blinkit_Item_id) as string)) as blinkit_sku, upper(cast(trim(Instamart_Item_Code) as string)) as swiggy_sku, upper(cast(trim(SKUCODE) as string)) as master_sku, upper(cast(trim(PRODUCT_TITLE) as string)) as product_name, upper(cast(trim(Category) as string)) as category, upper(cast(trim(Sub_Category) as string)) as sub_category, upper(cast(trim(Style) as string)) as style, upper(cast(trim(NATURE) as string)) as nature from maplemonk.Prolicious_GS_SKU_MASTER ;",
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
            