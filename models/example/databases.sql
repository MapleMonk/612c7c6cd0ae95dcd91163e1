{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE bsc_db.Maplemonk.bombae_GA_ORDER_BY_SOURCE_CONSOLIDATED AS select \'SHOPIFY\' as Shop_Name ,GCM.type as channel_type ,upper(coalesce(GCM.final_channel, \'unmapped\' )) Channel ,upper(coalesce(GCM.final_source, \'unmapped\')) Final_Source ,to_date(GASC.date, \'yyyymmdd\') date ,GASC.medium ,GASC.source ,GASC.sourcemedium ,GASC.transactionid ,GASC.grosspurchaserevenue from bsc_db.Maplemonk.GOOGLE_ANALYTICS_4__GA4__GA4_bombae_ORDERS_BY_SOURCE GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, final_source, type,row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from bsc_db.Maplemonk.ga_channel_mapping) where rw=1) GCM on lower(GASC.sourceMedium) = lower(GCM.GA_SOURCEMEDIUM) ; CREATE OR REPLACE TABLE bsc_db.Maplemonk.BOMBAE_GA_SESSIONS_CONSOLIDATED AS select \'SHOPIFY\' as Shop_Name ,GCM.type as channel_type ,upper(coalesce(GCM.final_channel, \'unmapped\' )) Channel ,upper(coalesce(GCM.final_source, \'unmapped\')) Final_Source ,to_Date(GASC.date, \'yyyymmdd\') date ,GASC.sessions ,GASC.totalusers ,GASC.property_id ,GASC.engagedsessions ,GASC.screenpageviews ,GASC.sessionsourcemedium ,GASC.averagesessionduration ,GASC.screenpageviewspersession from bsc_db.Maplemonk.Google_Analytics_4__GA4__GA4_bombae_SESSIONS_USERS_BY_DATE GASC left join (select * from (select ga_source, ga_medium, ga_sourcemedium,final_channel, final_source, type, row_number() over (partition by lower(ga_sourcemedium) order by lower(ga_sourcemedium)) rw from bsc_db.Maplemonk.ga_channel_mapping) where rw=1) GCM on lower(GASC.sessionSourceMedium) = lower(GCM.ga_sourcemedium);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        