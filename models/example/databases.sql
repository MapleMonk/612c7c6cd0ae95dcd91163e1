{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.ZOUK_MYNTRA_ADS_FACT_ITEMS AS select \'MYNTRA\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(CTR as FLOAT64) as CTR ,SAFE_CAST(CVR as FLOAT64) as CVR ,date as date1 ,COALESCE(SAFE.PARSE_DATE(\'%d-%b-%Y\', Date),SAFE.PARSE_DATE(\'%d/%m/%Y\', Date),SAFE.PARSE_DATE(\'%d-%m-%Y\', Date)) AS Date ,SAFE_CAST(Views as FLOAT64) as Views ,SAFE_CAST(Clicks as FLOAT64) as Clicks ,Product_ID ,SKU.WMS_SKU ,Ad_Group_ID ,Campaign_ID ,SAFE_CAST(Impressions as FLOAT64) as Impressions ,upper(coalesce(fsm.Name, MPA.Product_Name)) Product_Name ,fsm.category ,fsm.collection ,SAFE_CAST(ROI__Direct_ as FLOAT64) as ROI_Direct ,Ad_Group_Name ,Campaign_Name ,SAFE_CAST(ROI__Indirect_ as FLOAT64) as ROI_Indirect ,SAFE_CAST(Units_Sold__Direct_ as FLOAT64) as Units_Sold_Direct ,SAFE_CAST(Units_Sold__InDirect_ as FLOAT64) as Units_Sold_InDirect ,SAFE_CAST(Advertiser_Spend_in_Currency__in_INR_ as FLOAT64) as Advertiser_Spend_in_Currency_in_INR ,SAFE_CAST(Revenue_in_Currency__Direct___in_INR_ as FLOAT64) as Revenue_in_Currency_Direct_in_INR ,SAFE_CAST(Revenue_in_Currency__Indirect___in_INR_ as FLOAT64) as Revenue_in_Currency_Indirect_in_INR ,_ab_source_file_url from `MapleMonk.ZOUK_MYNTRA_PRODUCT_ADS` MPA left join (select WMS_SKU, Myntra from maplemonk.sku_mapping qualify row_number() over(partition by lower(myntra) order by 1) = 1 ) SKU on MPA.Product_ID = SKU.Myntra left join ( select * from maplemonk.final_sku_master qualify row_number() over(partition by lower(COMMONSKU) order by 1) = 1 )fsm on lower(SKU.WMS_SKU) = lower(fsm.COMMONSKU) where date <> \'\' ;",
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
            