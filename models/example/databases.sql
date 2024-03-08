{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA4_Order_By_Source_Consolidated_Intermediate AS select \'Shopify_DRV\' as Shop_Name ,to_date(date,\'yyyymmdd\') DATE ,MEDIUM GA4_MEDIUM ,SOURCE GA4_SOURCE ,PROPERTY_ID ,SOURCEMEDIUM GA4_SOURCEMEDIUM ,TRANSACTIONID ,GROSSPURCHASEREVENUE ,_airbyte_emitted_at from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,transactionid,source,medium,sourcemedium order by _airbyte_emitted_at desc)rw from RPSG_DB.MAPLEMONK.ga4_drv__orders_by_source ) ) union all select \'Shopify_Herbobuild\' as Shop_Name ,to_date(date,\'yyyymmdd\') DATE ,MEDIUM GA4_MEDIUM ,SOURCE GA4_SOURCE ,PROPERTY_ID ,SOURCEMEDIUM GA4_SOURCEMEDIUM ,TRANSACTIONID ,GROSSPURCHASEREVENUE ,_airbyte_emitted_at from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,transactionid,source,medium,sourcemedium order by _airbyte_emitted_at desc)rw from RPSG_DB.MAPLEMONK.ga4_hb_orders_by_source ) where rw=1 and date < \'2023-12-01\') union all select \'Shopify_AyurvedicSource\' as Shop_Name ,to_date(date,\'yyyymmdd\') DATE ,MEDIUM GA4_MEDIUM ,SOURCE GA4_SOURCE ,PROPERTY_ID ,SOURCEMEDIUM GA4_SOURCEMEDIUM ,TRANSACTIONID ,GROSSPURCHASEREVENUE ,_airbyte_emitted_at from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,transactionid,source,medium,sourcemedium order by _airbyte_emitted_at desc)rw from RPSG_DB.MAPLEMONK.ga4_as_orders_by_source ) where rw=1 and date < \'2023-12-01\' ) ; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA4_ORDER_BY_SOURCE_CONSOLIDATED_DRV AS select coalesce(GCM.\"New Channel\", GASC.GA4_SOURCEMEDIUM) Channel ,GASC.* from RPSG_DB.MAPLEMONK.GA4_Order_By_Source_Consolidated_Intermediate GASC left join ( select * from (select *, row_number() over (partition by lower(ifnull(source,\'\')),lower(ifnull(medium,\'\')) order by \"New Channel\") rw from RPSG_DB.MAPLEMONK.utm_ga_consolidated_channel_mapping) where rw=1 and (source is not null or medium is not null) ) GCM on lower(ifnull(GASC.GA4_SOURCE,\'\')) = lower(ifnull(GCM.Source,\'\')) and lower(ifnull(GASC.GA4_MEDIUM,\'\')) = lower(ifnull(GCM.medium,\'\')) ; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA4_Sessions_Consolidated_Intermediate AS (select \'Shopify_DRV\' as Shop_Name ,to_date(DATE,\'yyyymmdd\') DATE ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,SESSIONMEDIUM GA4_SESSIONMEDIUM ,SESSIONSOURCE GA4_SESSIONSOURCE ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONCAMPAIGNNAME GA4_SESSIONCAMPAIGN ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessionmedium,sessionsource,sessionsourcemedium,sessioncampaignname order by _airbyte_emitted_at desc)rw from RPSG_DB.MAPLEMONK.ga4_drv__sessions_by_date_campaign ) where rw=1 ) where SESSIONCAMPAIGNNAME is not null order by 2 desc) union all (select \'Shopify_Herbobuild\' as Shop_Name ,to_date(date,\'yyyymmdd\') DATE ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,SESSIONMEDIUM GA4_SESSIONMEDIUM ,SESSIONSOURCE GA4_SESSIONSOURCE ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONCAMPAIGNNAME GA4_SESSIONCAMPAIGN ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessionmedium,sessionsource,sessionsourcemedium,sessioncampaignname order by _airbyte_emitted_at desc)rw from RPSG_DB.MAPLEMONK.ga4_hb_sessions_by_date_campaign ) where rw=1 and date < \'2023-12-01\') where SESSIONCAMPAIGNNAME is not null ) union all (select \'Shopify_AyurvedicSource\' as Shop_Name ,to_date(date,\'yyyymmdd\') DATE ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,SESSIONMEDIUM GA4_SESSIONMEDIUM ,SESSIONSOURCE GA4_SESSIONSOURCE ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONCAMPAIGNNAME GA4_SESSIONCAMPAIGN ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessionmedium,sessionsource,sessionsourcemedium,sessioncampaignname order by _airbyte_emitted_at desc)rw from RPSG_DB.MAPLEMONK.ga4_as_sessions_by_date_campaign ) where rw=1 and date < \'2023-12-01\' ) where SESSIONCAMPAIGNNAME is not null) ; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA4_Sessions_Consolidated_DRV AS select coalesce(GCM.\"New Channel\",\'Not Mapped\') Channel ,GASC.* from RPSG_DB.MAPLEMONK.GA4_Sessions_Consolidated_Intermediate GASC left join ( select * from (select *, row_number() over (partition by lower(ifnull(source,\'\')),lower(ifnull(medium,\'\')) order by \"New Channel\") rw from RPSG_DB.MAPLEMONK.utm_ga_consolidated_channel_mapping) where rw=1 and (source is not null or medium is not null) ) GCM on lower(ifnull(GASC.ga4_sessionsource ,\'\')) = lower(ifnull(GCM.source ,\'\')) and lower(ifnull(GASC.GA4_SESSIONMEDIUM,\'\')) = lower(ifnull(GCM.medium,\'\')); create or replace table RPSG_DB.MAPLEMONK.GA4_USERS_CONSOLIDATED_DRV AS ( select \'Shopify_DRV\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_drv__total_users_by_date) where rw=1) order by 2 desc ) union all ( select \'Shopify_AyurvedicSource\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_as_total_users_by_date) where rw=1 and date < \'2023-12-01\') ) union all (select \'Shopify_Herbobuild\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_hb_total_users_by_date) where rw=1 and date < \'2023-12-01\' )) ; create or replace table RPSG_DB.MAPLEMONK.GA4_USERS_CAMPAIGN_SOURCE_CONSOLIDATED_DRV AS select \'Shopify_DRV\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONCAMPAIGNNAME GA4_SESSIONCAMPAIGNNAME ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessionsourcemedium,sessioncampaignname order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_drv__users_by_date_campaign_source ) where rw=1) union all select \'Shopify_AyurvedicSource\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONCAMPAIGNNAME GA4_SESSIONCAMPAIGNNAME ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessioncampaignname,sessionsourcemedium order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.GA4_AS_USERS_BY_DATE_CAMPAIGN_SOURCE) where rw=1 and date < \'2023-12-01\' ) union all select \'Shopify_Herbobuild\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONCAMPAIGNNAME GA4_SESSIONCAMPAIGNNAME ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessioncampaignname,sessionsourcemedium order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_hb_users_by_date_campaign_source) where rw=1 and date < \'2023-12-01\' ) ; create or replace table RPSG_DB.MAPLEMONK.GA4_USERS_SOURCE_CONSOLIDATED_DRV AS select \'Shopify_DRV\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessionsourcemedium order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_drv__users_by_date_source ) where rw=1) union all select \'Shopify_AyurvedicSource\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessionsourcemedium order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_as_users_by_date_source ) where rw=1 and date < \'2023-12-01\' ) union all select \'Shopify_Herbobuild\' as Shop_Name ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from (select * from(select *,row_number() over(partition by date,PROPERTY_ID,sessionsourcemedium order by _airbyte_emitted_at desc)rw from rpsg_db.maplemonk.ga4_hb_users_by_date_source ) where rw=1 and date < \'2023-12-01\' ) ;",
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
                        