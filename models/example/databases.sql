{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Nykaa_Ads_Factitems AS select \'NYKAA\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(CTR as FLOAT64) as CTR ,ad_live_date as date1 ,COALESCE(SAFE.PARSE_DATE(\'%m-%d-%Y\', ad_live_date),SAFE.PARSE_DATE(\'%m/%d/%Y\', ad_live_date),SAFE.PARSE_DATE(\'%m-%d-%y\', ad_live_date),SAFE.PARSE_DATE(\'%Y-%m-%d\', ad_live_date)) AS Date ,SAFE_CAST(Clicks as FLOAT64) as Clicks ,NYKAA ,Product_ID ,SKU.WMS_SKU ,SAFE_CAST(Impressions as FLOAT64) as Impressions ,upper(coalesce(fsm.Name, NA.Product_Name)) Product_Name ,fsm.category ,fsm.collection ,upper(brand_name) AS Brand ,campaign as Campaign_Name ,SAFE_CAST(spends as FLOAT64) as Spends ,SAFE_CAST(Revenue as FLOAT64) as Revenue from `MapleMonk.Zouk_Nykaa_Ads_Upload` NA left join (select WMS_SKU, NYKAA from maplemonk.sku_mapping qualify row_number() over(partition by lower(NYKAA) order by 1) = 1 ) SKU on lower(NA.Product_ID) = lower(SKU.NYKAA) left join ( select * from maplemonk.final_sku_master qualify row_number() over(partition by lower(COMMONSKU) order by ifnull(collection,\'\') desc) = 1 )fsm on lower(SKU.WMS_SKU) = lower(fsm.COMMONSKU) where ad_live_date <> \'\'",
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
            