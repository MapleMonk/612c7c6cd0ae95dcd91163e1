{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_final_sku_master as with listing_master as ( select upper(Marketplace) MARKETPLACE ,upper(replace(Marketplace_SKU,\'`\',\'\')) MARKETPLACE_SKU ,upper(replace(Master_SKU,\'`\',\'\')) COMMONSKU from (Select * from `MAPLEMONK.Saadaa_GS_SKU_LISTING_MASTER` where not(lower(marketplace) like \'%amazon smart connect%\')) qualify row_number() over (partition by upper(marketplace),upper(replace(Marketplace_SKU,\'`\',\'\')) order by length(replace(Master_SKU,\'`\',\'\')) desc) = 1 and replace(Master_SKU,\'`\',\'\') <> \'\' ), commonskumaster as ( select upper(replace(SKU,\'`\',\'\')) COMMONSKU ,PRODUCT_ID ,upper(product_name) PRODUCT_NAME ,upper(category_name) CATEGORY ,upper(PRODUCT_TYPE) PRODUCT_TYPE ,upper(SIZE) SIZE ,Upper(colour) COLOR ,cast(mrp as float64) MRP ,cast(cost as float64) COST ,cast(tax_rate as float64) TAX_RATE ,cast(weight as float64) WEIGHT ,cast(height as float64) HEIGHT ,cast(length as float64) LENGTH from `MAPLEMONK.EasyEcom_SAADAA_product_master` ) select lm.MARKETPLACE ,lm.MARKETPLACE_SKU ,lm.COMMONSKU ,cm.PRODUCT_ID ,CM.PRODUCT_NAME ,CM.CATEGORY ,CM.PRODUCT_TYPE ,CM.SIZE ,CM.COLOR ,CM.MRP ,CM.COST ,CM.TAX_RATE ,CM.WEIGHT ,CM.HEIGHT ,CM.LENGTH from listing_master lm left join commonskumaster cm on lm.COMMONSKU = cm.COMMONSKU ;",
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
            