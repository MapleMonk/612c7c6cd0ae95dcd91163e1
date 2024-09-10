{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Marketing_Consolidated_FR AS Select upper(adGroupName) adGroupName ,ADID ::varchar as ad_id ,null as ACCOUNT_NAME ,null as ACCOUNT_ID ,upper(CAMPAIGNNAME) as CAMPAIGN_NAME ,CAMPAIGNID ::varchar as CAMPAIGN_ID ,DATE ::DATE DATE ,CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,dayname(try_to_date(date)) DAY_OF_WEEK ,year(try_to_date(date)) YEAR ,month(try_to_date(date)) MONTH ,\'AMAZON\' AS CHANNEL ,adGroupId::varchar AS ACCOUNT ,sum(ifnull(CLICKS,0)) CLICKS ,sum(ifnull(CLICKS,0)) OUTBOUND_CLICKS ,sum(ifnull(SPEND,0)) as SPEND ,sum(ifnull(IMPRESSIONS,0))IMPRESSIONS ,sum(ifnull(CONVERSIONS,0)) CONVERSIONS ,sum(ifnull(SALES,0)) AdSales from VAHDAM_DB.MAPLEMONK.AMAZONADS_FR_MARKETING group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from VAHDAM_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            