{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rpsg_db.maplemonk.GA_FINAL_ORDER_BY_SOURCE_CONSOLIDATED_DRV as select CHANNEL ,SHOP_NAME ,GA_DATE ,VIEW_ID ,GA_MEDIUM ,GA_SOURCE ,GA_SOURCEMEDIUM ,GA_TRANSACTIONID ,GA_REVENUEPERTRANSACTION from RPSG_DB.MAPLEMONK.GA_ORDER_BY_SOURCE_CONSOLIDATED_DRV where ga_date < \'2023-06-01\' union all select CHANNEL ,SHOP_NAME ,DATE ,PROPERTY_ID ,GA4_MEDIUM ,GA4_SOURCE ,GA4_SOURCEMEDIUM ,TRANSACTIONID ,GROSSPURCHASEREVENUE from RPSG_DB.MAPLEMONK.GA4_ORDER_BY_SOURCE_CONSOLIDATED_DRV where date >= \'2023-06-01\'; create or replace table RPSG_DB.MAPLEMONK.GA_Final_Sessions_Consolidated_DRV as select CHANNEL ,SHOP_NAME ,GA_DATE ,VIEW_ID ,GA_EXITS ,GA_USERS ,GA_MEDIUM ,GA_SOURCE ,GA_CAMPAIGN ,GA_NEWUSERS ,GA_SESSIONS ,GA_PAGEVIEWS ,GA_SOURCEMEDIUM ,GA_UNIQUEPAGEVIEWS ,GA_AVGSESSIONDURATION ,0 as GA_ENGAGEDSESSIONS from RPSG_DB.MAPLEMONK.GA_Sessions_Consolidated_DRV where ga_date < \'2023-06-01\' union all select CHANNEL ,SHOP_NAME ,DATE ,PROPERTY_ID ,0 as EXITS ,TOTALUSERS ,GA4_SESSIONMEDIUM ,GA4_SESSIONSOURCE ,GA4_SESSIONCAMPAIGN ,NEWUSERS ,SESSIONS ,SCREENPAGEVIEWS ,GA4_SESSIONSOURCEMEDIUM ,SCREENPAGEVIEWS ,AVERAGESESSIONDURATION ,ENGAGEDSESSIONS from RPSG_DB.MAPLEMONK.GA4_Sessions_Consolidated_DRV where date >= \'2023-06-01\'; create or replace table RPSG_DB.MAPLEMONK.GA_FINAL_USERS_CONSOLIDATED_DRV as select SHOP_NAME ,GA_DATE ,VIEW_ID ,TOTAL_USERS ,TOTAL_NEW_USERS ,TOTAL_EXITS ,TOTAL_SESSIONS ,TOTAL_PAGEVIEWS ,TOTAL_UNIQUEPAGEVIEWS ,TOTAL_BOUNCES ,TOTAL_AVG_SESSION_DURATION ,TOTAL_AVG_LOAD_TIME ,TOTAL_PAGEVIEW_PER_SESSION ,0 as TOTAL_ENGAGEDSESSIONS from RPSG_DB.MAPLEMONK.GA_USERS_CONSOLIDATED_DRV where ga_date < \'2023-06-01\' union all select SHOP_NAME ,DATE ,PROPERTY_ID ,TOTALUSERS ,NEWUSERS ,0 as Exits ,SESSIONS ,SCREENPAGEVIEWS ,SCREENPAGEVIEWS ,(ifnull(SESSIONS,0) - ifnull(ENGAGEDSESSIONS,0)) as BOUNCES ,AVERAGESESSIONDURATION ,0 as AVG_LOAD_TIME ,SCREENPAGEVIEWSPERSESSION ,ENGAGEDSESSIONS from RPSG_DB.MAPLEMONK.GA4_USERS_CONSOLIDATED_DRV where date >= \'2023-06-01\'; create or replace table RPSG_DB.MAPLEMONK.GA_FINAL_USERS_CAMPAIGN_SOURCE_CONSOLIDATED_DRV as select SHOP_NAME ,GA_DATE ,VIEW_ID ,GA_CAMPAIGN ,GA_SOURCEMEDIUM ,USERS_CAMPAIGN_SOURCE ,BOUNCES_CAMPAIGN_SOURCE ,SESSIONS_CAMPAIGN_SOURCE ,NEW_USERS_CAMPAIGN_SOURCE ,PAGEVIEWS_CAMPAIGN_SOURCE ,AVG_SESSION_DURATION_CAMPAIGN_SOURCE ,PAGEVIEW_PER_SESSION_CAMPAIGN_SOURCE ,EXITS_CAMPAIGN_SOURCE ,AVG_LOAD_TIME_CAMPAIGN_SOURCE ,(ifnull(SESSIONS_CAMPAIGN_SOURCE,0)-ifnull(BOUNCES_CAMPAIGN_SOURCE,0)) ENGAGED_SESSIONS_CAMPAIGN_SOURCE from RPSG_DB.MAPLEMONK.GA_USERS_CAMPAIGN_SOURCE_CONSOLIDATED_DRV where ga_date < \'2023-06-01\' union all select SHOP_NAME ,DATE ,PROPERTY_ID ,GA4_SESSIONCAMPAIGNNAME ,GA4_SESSIONSOURCEMEDIUM ,TOTALUSERS ,(ifnull(SESSIONS,0) -ifnull(ENGAGEDSESSIONS,0)) as BOUNCES ,SESSIONS ,NEWUSERS ,SCREENPAGEVIEWS ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION ,0 EXITS ,0 as LOAD_TIME ,ENGAGEDSESSIONS from RPSG_DB.MAPLEMONK.GA4_USERS_CAMPAIGN_SOURCE_CONSOLIDATED_DRV where date >= \'2023-06-01\'; create or replace table RPSG_DB.MAPLEMONK.GA_FINAL_USERS_SOURCE_CONSOLIDATED_DRV as select SHOP_NAME ,GA_DATE ,VIEW_ID ,GA_SOURCEMEDIUM ,USERS_SOURCE ,BOUNCES_SOURCE ,SESSIONS_SOURCE ,NEW_USERS_SOURCE ,PAGEVIEWS_SOURCE ,AVG_SESSION_DURATION_SOURCE ,PAGEVIEW_PER_SESSION_SOURCE ,EXITS_SOURCE ,AVG_LOAD_TIME_SOURCE ,(ifnull(SESSIONS_SOURCE,0)-ifnull(BOUNCES_SOURCE,0)) as ENGAGEDSESSIONS from RPSG_DB.MAPLEMONK.GA_USERS_SOURCE_CONSOLIDATED_DRV where ga_date < \'2023-06-01\' union all select SHOP_NAME ,DATE ,PROPERTY_ID ,GA4_SESSIONSOURCEMEDIUM ,TOTALUSERS ,(ifnull(SESSIONS,0) -ifnull(ENGAGEDSESSIONS,0)) as BOUNCES ,SESSIONS ,NEWUSERS ,SCREENPAGEVIEWS ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION ,0 as EXITS ,0 as LOAD_TIME ,ENGAGEDSESSIONS from RPSG_DB.MAPLEMONK.GA4_USERS_SOURCE_CONSOLIDATED_DRV where date >= \'2023-06-01\';",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        