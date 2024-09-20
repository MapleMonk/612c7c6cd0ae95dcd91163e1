{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table VAHDAM_DB.MAPLEMONK.GA4_USERS_SOURCE_CONSOLIDATED_INTERMEDIATE_VT AS select \'SHOPIFY_USA\' as Shop_Name ,\'USA\' AS GEOGRAPHY ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from VAHDAM_DB.MAPLEMONK.ga4_USA_users_by_date_source union all select \'SHOPIFY_DE\' as Shop_Name ,\'DE\' AS GEOGRAPHY ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from VAHDAM_DB.MAPLEMONK.ga4_DE_users_by_date_source union all select \'SHOPIFY_GLOBAL\' as Shop_Name ,\'GLOBAL\' AS GEOGRAPHY ,to_date(date,\'yyyymmdd\') Date ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION ,SCREENPAGEVIEWSPERSESSION from VAHDAM_DB.MAPLEMONK.ga4_GLOBAL_users_by_date_source; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.GA4_USERS_SOURCE_CONSOLIDATED_VT AS select UPPER(coalesce(GCM.MAPPED_CHANNEL, GASC.GA4_SESSIONSOURCEMEDIUM)) Channel ,UPPER(GCM.MAPPED_CHANNEL) GA_CHANNEL ,GASC.* from VAHDAM_DB.MAPLEMONK.GA4_USERS_SOURCE_CONSOLIDATED_INTERMEDIATE_VT GASC left join (select * from (select *, row_number() over (partition by lower(ga_sourcemedium) order by 1) rw from VAHDAM_DB.MAPLEMONK.GA_MAPPING) where rw=1) GCM on lower(GASC.GA4_SESSIONSOURCEMEDIUM) = lower(GCM.GA_SOURCEMEDIUM);",
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
            