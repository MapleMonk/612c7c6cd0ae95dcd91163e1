{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.KA_Amazon_Vendor_Central_Traffic_Fact_Items as select cast(startDate as date) DATE ,\'AMAZON VENDOR CENTRAL INDIA\' Marketplace ,\'INDIA\' REGION , asin , p.skucode commonsku , upper(p.name) Product_Name , upper(p.category) Product_Category , upper(p.sub_category) Product_Sub_Category , cast(glanceViews as int64) glanceViews from `MapleMonk.KA_AVP_GET_VENDOR_TRAFFIC_REPORT` fi left join (select * from `MapleMonk.KA_GS_SKU_MarketplaceSKU_Mapping` qualify row_number()over (partition by AMAZON_ASIN order by 1) = 1 ) AM on upper(fi.ASIN) = upper(AM.AMAZON_ASIN) left join (select commonsku skucode, name, category, category_code sub_category, category_code, commonsku, TAX_RATE from maplemonk.final_sku_master qualify row_number()over (partition by commonsku order by 1) = 1 ) p on lower(AM.COMMONSKUCODE) = lower(p.skucode) ;",
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
            