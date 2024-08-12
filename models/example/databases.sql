{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.ZOUK_FLIPKART_ADS_FACT_ITEMS AS select \'FLIKART\' CHANNEL ,\'PCA ADS\' AD_TYPE ,NULL AS PRODUCT_ID ,NULL AS PRODUCT_NAME ,SAFE_CAST(CTR as FLOAT64) as CTR ,SAFE_CAST(CVR as FLOAT64) as CVR ,PARSE_DATE(\'%d-%b-%Y\', Date) Date ,SAFE_CAST(views as FLOAT64) as views ,SAFE_CAST(clicks as FLOAT64) as clicks ,SAFE_CAST(banner_group_spend as FLOAT64) as banner_group_spend ,ad_group_id ,campaign_id ,ad_group_name ,SAFE_CAST(Direct_ROI as FLOAT64) as Direct_ROI ,campaign_name ,SAFE_CAST(DIRECT_REVENUE as FLOAT64) as DIRECT_REVENUE ,SAFE_CAST(Indirect_ROI as FLOAT64) as Indirect_ROI ,SAFE_CAST(INDIRECT_REVENUE as FLOAT64) as INDIRECT_REVENUE ,SAFE_CAST(DIRECT_UNITS as FLOAT64) as DIRECT_UNITS ,SAFE_CAST(INDIRECT_UNITS as FLOAT64) as INDIRECT_UNITS ,SAFE_CAST(DIRECT_PPV as FLOAT64) as DIRECT_PPV ,SAFE_CAST(average_cpc as FLOAT64) as average_cpc ,_ab_source_file_url from `MapleMonk.ZOUK_FLIPKART_PCA_ADS` where date <> \'\' UNION ALL select \'FLIPKART\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,Advertised_FSN_ID ,upper(Advertised_Product_Name) Advertised_Product_Name ,SAFE_CAST(CTR as FLOAT64) as CTR ,SAFE_CAST(CVR as FLOAT64) as CVR ,PARSE_DATE(\'%d-%b-%Y\', Date) Date ,SAFE_CAST(Views as FLOAT64) as Views ,SAFE_CAST(Clicks as FLOAT64) as Clicks ,SAFE_CAST(Ad_Spend as FLOAT64) as Ad_Spend ,Ad_Group_ID ,Campaign_ID ,AdGroup_Name ,SAFE_CAST(ROI__Direct_ as FLOAT64) as ROI_Direct ,upper(Campaign_Name) Campaign_Name ,SAFE_CAST(Direct_Revenue as FLOAT64) as Direct_Revenue ,SAFE_CAST(ROI__Indirect_ as FLOAT64) as ROI_Indirect ,SAFE_CAST(Indirect_Revenue as FLOAT64) as Indirect_Revenue ,SAFE_CAST(Units_Sold__Direct_ as FLOAT64) as Units_Sold_Direct ,SAFE_CAST(Units_Sold__Indirect_ as FLOAT64) as Units_Sold_Indirect ,null ,null ,_ab_source_file_url from `MapleMonk.ZOUK_FLIPKART_PL_ADS` ;",
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
            