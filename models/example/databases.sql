{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE OFFDUTY_DB.MAPLEMONK.OFFDUTY_GA4_Sessions_Consolidated_Intermediate AS select \'SHOPIFY_OFFDUTYSTORE\' as Shop_Name ,to_date(date,\'yyyymmdd\') DATE ,NEWUSERS ,SESSIONS ,TOTALUSERS ,PROPERTY_ID ,SESSIONMEDIUM GA4_SESSIONMEDIUM ,SESSIONSOURCE GA4_SESSIONSOURCE ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM GA4_SESSIONSOURCEMEDIUM ,AVERAGESESSIONDURATION from OFFDUTY_DB.MAPLEMONK.offduty_ga4_sessionsby_date ; CREATE OR REPLACE TABLE OFFDUTY_DB.MAPLEMONK.OFFDUTY_DB_GA4_Sessions_Consolidated AS select upper(coalesce(GCM.final_channel,GA4_SESSIONSOURCEMEDIUM)) Channel ,upper(coalesce(GCM.final_channel,GA4_SESSIONSOURCE)) SOURCE ,GASC.* from OFFDUTY_DB.MAPLEMONK.OFFDUTY_GA4_Sessions_Consolidated_Intermediate GASC left join (select * from (select *, row_number() over (partition by lower(ga_sourcemedium) order by 1) rw from OFFDUTY_DB.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.GA4_SESSIONSOURCEMEDIUM) = lower(GCM.GA_SOURCEMEDIUM);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from offduty_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        