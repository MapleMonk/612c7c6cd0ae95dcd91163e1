{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.final_SKU_MASTER as select upper(cast(trim(NYKAA_SKU_CODE) as string)) as nykaa_sku, upper(cast(trim(ASN) as string)) as amazon_sku, upper(cast(trim(MYNTRA_SID) as string)) as myntra_sku, upper(cast(trim(TIRA_SAP_CODE) as string)) as tira_sku, upper(cast(trim(PRIMARYKEY) as string)) as master_sku, upper(cast(trim(PRODUCT_TITLE) as string)) as product_name, upper(cast(trim(Category) as string)) as category, upper(cast(trim(Sub_Category) as string)) as sub_category from maplemonk.skinbae_SKU_MASTER ;",
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
            