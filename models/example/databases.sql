{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.THEMOMSTORE_final_sku_master as ( With CTE as ( select trim(ASN) as marketplace_SK ,cast(null as string) as marketplace_id ,trim(style) as style ,replace(PRIMARYKEY,\'-\',\' \') as Product_Name ,Category ,\'Amazon\' as Marketplace ,NATURE ,Sub_Category ,AbsoluteKey from maplemonk.tms_sku_master qualify row_number() over(partition by trim(ASN) order by 1) = 1 UNION ALL select trim(NYKAA_SKU_CODE) as marketplace_SK ,trim(nykaa) as marketplace_id ,trim(style) as style ,replace(PRIMARYKEY,\'-\',\' \') as Product_Name ,Category ,\'Nykaa\' as Marketplace ,NATURE ,Sub_Category ,AbsoluteKey from maplemonk.tms_sku_master qualify row_number() over(partition by trim(NYKAA_SKU_CODE) order by 1) = 1 UNION ALL select trim(MYNTRA_SKU_Code) as marketplace_SK ,trim(MYNTRA_SID) as marketplace_id ,trim(style) as style ,replace(PRIMARYKEY,\'-\',\' \') as Product_Name ,Category ,\'Myntra\' as Marketplace ,NATURE ,Sub_Category ,AbsoluteKey from maplemonk.tms_sku_master qualify row_number() over(partition by trim(MYNTRA_SID) order by 1) = 1 UNION ALL select trim(FLIPKART_FSN) as marketplace_SK ,trim(FLIPKART_LID) as marketplace_id ,trim(style) as style ,replace(PRIMARYKEY,\'-\',\' \') as Product_Name ,Category ,\'Flipkart\' as Marketplace ,NATURE ,Sub_Category ,AbsoluteKey from maplemonk.tms_sku_master qualify row_number() over(partition by trim(FLIPKART_FSN) order by 1) = 1 ) Select * from CTE qualify row_number() over(partition by lower(ifnull(trim(style),\'\')), marketplace order by 1) = 1 );",
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
            