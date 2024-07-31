{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table VAHDAM_DB.maplemonk.AAMS_DE_SB_CONSOLIDATED_HOURLY_DATA as with SB_TRAFFIC_HOURLY as (select ADVERTISER_ID ,MARKETPLACE_ID ,profileid ,TIME_WINDOW_START::datetime Date ,hour(TIME_WINDOW_START::datetime) Hour ,AD_ID ,KEYWORD_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,KEYWORD_TEXT ,KEYWORD_TYPE ,PLACEMENT_TYPE ,CURRENCY ,sum(COST) SPEND ,sum(IMPRESSIONS) IMPRESSIONS ,sum(CLICKS) CLICKS ,sum(VIEWABLE_IMPRESSIONS) VIEWABLE_IMPRESSIONS from VAHDAM_DB.maplemonk.AAMS_DE_SB_TRAFFIC group by 1,2,3,4,5,6,7,8,9,10,11,12,13) , SB_CONVERSIONS_HOURLY as ( select ADVERTISER_ID ,MARKETPLACE_ID ,TIME_WINDOW_START::datetime Date ,hour(TIME_WINDOW_START::datetime) Hour ,AD_ID ,KEYWORD_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,profileid ,KEYWORD_TEXT ,KEYWORD_TYPE ,PLACEMENT_TYPE ,CURRENCY ,sum(ATTRIBUTED_SALES_14D) ATTRIBUTED_SALES_14D ,sum(ATTRIBUTED_UNITS_ORDERED_14D) ATTRIBUTED_UNITS_ORDERED_14D ,sum(ATTRIBUTED_CONVERSIONS_14D) ATTRIBUTED_CONVERSIONS_14D ,sum(VIEW_ATTRIBUTED_SALES_14D) VIEW_ATTRIBUTED_SALES_14D ,sum(VIEW_ATTRIBUTED_UNITS_ORDERED_14D) VIEW_ATTRIBUTED_UNITS_ORDERED_14D ,sum(VIEW_ATTRIBUTED_CONVERSIONS_14D) VIEW_ATTRIBUTED_CONVERSIONS_14D ,sum(ATTRIBUTED_SALES_14D_SAME_SKU) ATTRIBUTED_SALES_14D_SAME_SKU ,sum(ATTRIBUTED_CONVERSIONS_14D_SAME_SKU) ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,sum(ATTRIBUTED_SALES_NEW_TO_BRAND_14D) ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,sum(ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D) ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,sum(ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D) ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,sum(VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D) VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,sum(VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D) VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,sum(VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D) VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D from VAHDAM_DB.maplemonk.AAMS_DE_SB_CONVERSION group by 1,2,3,4,5,6,7,8,9,10,11,12,13 ) select coalesce(a.ADVERTISER_ID, b.ADVERTISER_ID) ADVERTISER_ID ,coalesce(a.MARKETPLACE_ID, b.MARKETPLACE_ID) MARKETPLACE_ID ,coalesce(a.profileid, b.profileid) Profileid ,coalesce(a.Date, b.Date) Date ,coalesce(a.Hour, b.Hour) Hour ,coalesce(a.AD_ID, b.AD_ID) AD_ID ,coalesce(a.KEYWORD_ID, b.KEYWORD_ID) KEYWORD_ID ,coalesce(a.AD_GROUP_ID, b.AD_GROUP_ID) AD_GROUP_ID ,coalesce(a.CAMPAIGN_ID, b.CAMPAIGN_ID) CAMPAIGN_ID ,coalesce(a.KEYWORD_TEXT, b.KEYWORD_TEXT) KEYWORD_TEXT ,coalesce(a.KEYWORD_TYPE, b.KEYWORD_TYPE) KEYWORD_TYPE ,coalesce(a.PLACEMENT_TYPE, b.PLACEMENT_TYPE) PLACEMENT_TYPE ,coalesce(a.CURRENCY, b.CURRENCY) CURRENCY ,b.ATTRIBUTED_SALES_14D ,b.ATTRIBUTED_UNITS_ORDERED_14D ,b.ATTRIBUTED_CONVERSIONS_14D ,b.VIEW_ATTRIBUTED_SALES_14D ,b.VIEW_ATTRIBUTED_UNITS_ORDERED_14D ,b.VIEW_ATTRIBUTED_CONVERSIONS_14D ,b.ATTRIBUTED_SALES_14D_SAME_SKU ,b.ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,b.ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,b.ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,b.ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,b.VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,b.VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,b.VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,a.SPEND ,a.IMPRESSIONS ,a.CLICKS ,a.VIEWABLE_IMPRESSIONS from SB_TRAFFIC_HOURLY a full outer join SB_CONVERSIONS_HOURLY b on a.MARKETPLACE_ID = b.MARKETPLACE_ID and a.Date = b.Date and a.AD_ID = b.AD_ID and a.KEYWORD_ID = b.KEYWORD_ID and a.AD_GROUP_ID = b.AD_GROUP_ID and a.CAMPAIGN_ID = b.CAMPAIGN_ID and a.profileid = b.profileid and lower(a.PLACEMENT_TYPE) = lower(b.PLACEMENT_TYPE); create or replace table VAHDAM_DB.maplemonk.AAMS_DE_SD_CONSOLIDATED_HOURLY_DATA as with SD_TRAFFIC_HOURLY as (select ADVERTISER_ID ,MARKETPLACE_ID ,profileid ,TIME_WINDOW_START::datetime Date ,hour(TIME_WINDOW_START::datetime) Hour ,AD_ID ,TARGET_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,TARGETING_TEXT ,CURRENCY ,sum(COST) SPEND ,sum(IMPRESSIONS) IMPRESSIONS ,sum(CLICKS) CLICKS ,sum(VIEW_IMPRESSIONS) VIEWABLE_IMPRESSIONS from VAHDAM_DB.maplemonk.AAMS_DE_SD_TRAFFIC group by 1,2,3,4,5,6,7,8,9,10,11), SD_CONVERSIONS_HOURLY as ( select ADVERTISER_ID ,MARKETPLACE_ID ,profileid ,TIME_WINDOW_START::datetime Date ,hour(TIME_WINDOW_START::datetime) Hour ,AD_ID ,TARGET_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,CURRENCY ,sum(ATTRIBUTED_SALES_14D) ATTRIBUTED_SALES_14D ,sum(ATTRIBUTED_UNITS_ORDERED_14D) ATTRIBUTED_UNITS_ORDERED_14D ,sum(ATTRIBUTED_CONVERSIONS_14D) ATTRIBUTED_CONVERSIONS_14D ,sum(VIEW_ATTRIBUTED_SALES_14D) VIEW_ATTRIBUTED_SALES_14D ,sum(VIEW_ATTRIBUTED_UNITS_ORDERED_14D) VIEW_ATTRIBUTED_UNITS_ORDERED_14D ,sum(VIEW_ATTRIBUTED_CONVERSIONS_14D) VIEW_ATTRIBUTED_CONVERSIONS_14D ,sum(ATTRIBUTED_SALES_14D_SAME_SKU) ATTRIBUTED_SALES_14D_SAME_SKU ,sum(ATTRIBUTED_CONVERSIONS_14D_SAME_SKU) ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,sum(ATTRIBUTED_SALES_NEW_TO_BRAND_14D) ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,sum(ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D) ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,sum(ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D) ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,sum(VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D) VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,sum(VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D) VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,sum(VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D) VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D from VAHDAM_DB.maplemonk.AAMS_DE_SD_CONVERSION group by 1,2,3,4,5,6,7,8,9,10 ) select coalesce(a.ADVERTISER_ID, b.ADVERTISER_ID) ADVERTISER_ID ,coalesce(a.MARKETPLACE_ID, b.MARKETPLACE_ID) MARKETPLACE_ID ,coalesce(a.profileid, b.profileid) Profileid ,coalesce(a.Date, b.Date) Date ,coalesce(a.Hour, b.Hour) Hour ,coalesce(a.AD_ID, b.AD_ID) AD_ID ,coalesce(a.TARGET_ID, b.TARGET_ID) TARGET_ID ,coalesce(a.AD_GROUP_ID, b.AD_GROUP_ID) AD_GROUP_ID ,coalesce(a.CAMPAIGN_ID, b.CAMPAIGN_ID) CAMPAIGN_ID ,a.TARGETING_TEXT ,coalesce(a.CURRENCY, b.CURRENCY) CURRENCY ,b.ATTRIBUTED_SALES_14D ,b.ATTRIBUTED_UNITS_ORDERED_14D ,b.ATTRIBUTED_CONVERSIONS_14D ,b.VIEW_ATTRIBUTED_SALES_14D ,b.VIEW_ATTRIBUTED_UNITS_ORDERED_14D ,b.VIEW_ATTRIBUTED_CONVERSIONS_14D ,b.ATTRIBUTED_SALES_14D_SAME_SKU ,b.ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,b.ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,b.ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,b.ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,b.VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,b.VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,b.VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,a.SPEND ,a.IMPRESSIONS ,a.CLICKS ,a.VIEWABLE_IMPRESSIONS from SD_TRAFFIC_HOURLY a full outer join SD_CONVERSIONS_HOURLY b on a.MARKETPLACE_ID = b.MARKETPLACE_ID and a.Date = b.Date and a.AD_ID = b.AD_ID and a.TARGET_ID = b.TARGET_ID and a.profileid = b.profileid and a.AD_GROUP_ID = b.AD_GROUP_ID and a.CAMPAIGN_ID = b.CAMPAIGN_ID ; create or replace table VAHDAM_DB.maplemonk.AAMS_DE_SP_CONSOLIDATED_HOURLY_DATA as with SP_TRAFFIC_HOURLY as (select ADVERTISER_ID ,MARKETPLACE_ID ,profileid ,TIME_WINDOW_START::datetime Date ,hour(TIME_WINDOW_START::datetime) Hour ,AD_ID ,KEYWORD_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,KEYWORD_TEXT ,MATCH_TYPE ,PLACEMENT ,CURRENCY ,sum(COST) SPEND ,sum(IMPRESSIONS) IMPRESSIONS ,sum(CLICKS) CLICKS from VAHDAM_DB.maplemonk.AAMS_DE_SP_TRAFFIC group by 1,2,3,4,5,6,7,8,9,10,11,12,13) , SP_CONVERSIONS_HOURLY as ( select ADVERTISER_ID ,MARKETPLACE_ID ,profileid ,TIME_WINDOW_START::datetime Date ,hour(TIME_WINDOW_START::datetime) Hour ,AD_ID ,KEYWORD_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,PLACEMENT ,CURRENCY ,sum(ATTRIBUTED_SALES_14D) ATTRIBUTED_SALES_14D ,sum(ATTRIBUTED_UNITS_ORDERED_14D) ATTRIBUTED_UNITS_ORDERED_14D ,sum(ATTRIBUTED_CONVERSIONS_14D) ATTRIBUTED_CONVERSIONS_14D ,sum(ATTRIBUTED_SALES_14D_SAME_SKU) ATTRIBUTED_SALES_14D_SAME_SKU ,sum(ATTRIBUTED_CONVERSIONS_14D_SAME_SKU) ATTRIBUTED_CONVERSIONS_14D_SAME_SKU from VAHDAM_DB.maplemonk.AAMS_DE_SP_CONVERSION group by 1,2,3,4,5,6,7,8,9,10,11 ) select coalesce(a.ADVERTISER_ID, b.ADVERTISER_ID) ADVERTISER_ID ,coalesce(a.MARKETPLACE_ID, b.MARKETPLACE_ID) MARKETPLACE_ID ,coalesce(a.profileid, b.profileid) Profileid ,coalesce(a.Date, b.Date) Date ,coalesce(a.Hour, b.Hour) Hour ,coalesce(a.AD_ID, b.AD_ID) AD_ID ,coalesce(a.KEYWORD_ID, b.KEYWORD_ID) KEYWORD_ID ,coalesce(a.AD_GROUP_ID, b.AD_GROUP_ID) AD_GROUP_ID ,coalesce(a.CAMPAIGN_ID, b.CAMPAIGN_ID) CAMPAIGN_ID ,a.KEYWORD_TEXT ,a.MATCH_TYPE ,a.PLACEMENT ,coalesce(a.CURRENCY, b.CURRENCY) CURRENCY ,b.ATTRIBUTED_SALES_14D ,b.ATTRIBUTED_UNITS_ORDERED_14D ,b.ATTRIBUTED_CONVERSIONS_14D ,b.ATTRIBUTED_SALES_14D_SAME_SKU ,b.ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,a.SPEND ,a.IMPRESSIONS ,a.CLICKS from SP_TRAFFIC_HOURLY a full outer join SP_CONVERSIONS_HOURLY b on a.MARKETPLACE_ID = b.MARKETPLACE_ID and a.Date = b.Date and a.AD_ID = b.AD_ID and a.KEYWORD_ID = b.KEYWORD_ID and a.profileid = b.profileid and a.AD_GROUP_ID = b.AD_GROUP_ID and a.CAMPAIGN_ID = b.CAMPAIGN_ID and lower(a.PLACEMENT) = lower(b.PLACEMENT) ; CREATE OR REPLACE TABLE VAHDAM_DB.maplemonk.AAMS_DE_HOURLY_DATA_CONSOLIDATED AS select o.* , cd.campaignname ,case when lower(PRE_CAMPAIGN_TYPE) like \'%brand%\' and lower(cd.campaignname) like \'%video%\' then \'Sponsored Brands Video\' else O.PRE_CAMPAIGN_TYPE end CAMPAIGN_TYPE ,cd.campaignstatus , adg.adgroupname ,case when lower(CAMPAIGN_TYPE) like \'%brand%\' then cd.asin else add.adname end ASIN , td.targeting_type , td.targeting_value from ( select ADVERTISER_ID ,MARKETPLACE_ID ,Profileid ,Date ,Hour ,\'Sponsored Brands\' PRE_CAMPAIGN_TYPE ,AD_ID ,KEYWORD_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,null as target_id ,KEYWORD_TEXT ,KEYWORD_TYPE ,null as TARGETING_TEXT ,PLACEMENT_TYPE ,CURRENCY ,ATTRIBUTED_SALES_14D ,ATTRIBUTED_UNITS_ORDERED_14D ,ATTRIBUTED_CONVERSIONS_14D ,VIEW_ATTRIBUTED_SALES_14D ,VIEW_ATTRIBUTED_UNITS_ORDERED_14D ,VIEW_ATTRIBUTED_CONVERSIONS_14D ,ATTRIBUTED_SALES_14D_SAME_SKU ,ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,SPEND ,IMPRESSIONS ,CLICKS ,VIEWABLE_IMPRESSIONS from VAHDAM_DB.maplemonk.AAMS_DE_SB_CONSOLIDATED_HOURLY_DATA union all select ADVERTISER_ID ,MARKETPLACE_ID ,Profileid ,Date ,Hour ,\'Sponsored Display\' PRE_CAMPAIGN_TYPE ,AD_ID ,null as keyword_id ,AD_GROUP_ID ,CAMPAIGN_ID ,TARGET_ID ,null as keyword_text ,null as KEYWORD_TYPE ,TARGETING_TEXT ,null as PLACEMENT_TYPE ,CURRENCY ,ATTRIBUTED_SALES_14D ,ATTRIBUTED_UNITS_ORDERED_14D ,ATTRIBUTED_CONVERSIONS_14D ,VIEW_ATTRIBUTED_SALES_14D ,VIEW_ATTRIBUTED_UNITS_ORDERED_14D ,VIEW_ATTRIBUTED_CONVERSIONS_14D ,ATTRIBUTED_SALES_14D_SAME_SKU ,ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,SPEND ,IMPRESSIONS ,CLICKS ,VIEWABLE_IMPRESSIONS from VAHDAM_DB.maplemonk.AAMS_DE_SD_CONSOLIDATED_HOURLY_DATA union all select ADVERTISER_ID ,MARKETPLACE_ID ,Profileid ,Date ,Hour ,\'Sponsored Products\' PRE_CAMPAIGN_TYPE ,AD_ID ,KEYWORD_ID ,AD_GROUP_ID ,CAMPAIGN_ID ,null as target_id ,KEYWORD_TEXT ,MATCH_TYPE ,null as target_text ,PLACEMENT ,CURRENCY ,ATTRIBUTED_SALES_14D ,ATTRIBUTED_UNITS_ORDERED_14D ,ATTRIBUTED_CONVERSIONS_14D ,null as VIEW_ATTRIBUTED_SALES_14D ,null as VIEW_ATTRIBUTED_UNITS_ORDERED_14D ,null as VIEW_ATTRIBUTED_CONVERSIONS_14D ,ATTRIBUTED_SALES_14D_SAME_SKU ,ATTRIBUTED_CONVERSIONS_14D_SAME_SKU ,null as ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,null as ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,null as ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,null as VIEW_ATTRIBUTED_SALES_NEW_TO_BRAND_14D ,null as VIEW_ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D ,null as VIEW_ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D ,SPEND ,IMPRESSIONS ,CLICKS ,null as VIEWABLE_IMPRESSIONS from VAHDAM_DB.maplemonk.AAMS_DE_SP_CONSOLIDATED_HOURLY_DATA )as o left join VAHDAM_DB.maplemonk.AAMS_DE_campaign_details cd on o.campaign_id = cd.campaignid left join VAHDAM_DB.maplemonk.AAMS_DE_adgroup_details adg on o.campaign_id = adg.campaignid and o.AD_GROUP_ID = adg.adgroupid left join VAHDAM_DB.maplemonk.AAMS_DE_ad_details add on o.campaign_id = add.campaignid and o.AD_GROUP_ID = add.adgroupid and o.ad_id = add.adid left join VAHDAM_DB.maplemonk.AAMS_DE_targeting_details td on o.campaign_id = td.campaignid and o.AD_GROUP_ID = td.adgroupid and o.target_id = td.targetid left join VAHDAM_DB.maplemonk.AAMS_DE_keyword_details kd on o.campaign_id = kd.campaignid and o.AD_GROUP_ID = kd.adgroupid and o.keyword_id = kd.keywordid ;",
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
            