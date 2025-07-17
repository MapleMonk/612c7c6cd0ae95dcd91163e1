{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE central-segment-458312-c3.maplemonk.THEMOMSTORE_GA_ORDER_BY_SOURCE_CONSOLIDATED AS select \'WEBSITE\' as Shop_Name ,GCM.final_channel Channel ,GCM.final_source Final_Source ,PARSE_DATE(\'%Y%m%d\', GASC.date) date ,GASC.SessionMedium ,GASC.SessionSource ,GASC.SessionSourceMedium ,GASC.transactionid ,GASC.grosspurchaserevenue from `central-segment-458312-c3.maplemonk.GA4_GA4_Themonstore_ORDERS_BY_SOURCE` GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, final_source, row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from central-segment-458312-c3.maplemonk.TMS_GA_CHANNEL_MAPPING) where rw=1) GCM on lower(replace(GASC.SessionSourceMedium,\' \',\'\')) = lower(replace(GCM.GA_SOURCEMEDIUM,\' \',\'\')) ; CREATE OR REPLACE TABLE central-segment-458312-c3.maplemonk.THEMOMSTORE_GA_Sessions_Consolidated AS select \'WEBSITE\' as Shop_Name ,GCM.final_channel Channel ,GCM.final_source Final_Source ,PARSE_DATE(\'%Y%m%d\', GASC.date) DATE ,GASC.sessions ,GASC.totalusers ,GASC.property_id ,GASC.engagedsessions ,GASC.screenpageviews ,GASC.sessionsourcemedium ,GASC.averagesessionduration ,GASC.screenpageviewspersession from `central-segment-458312-c3.maplemonk.GA4_GA4_THEMONSTORE_SESSIONS_USERS_BY_DATE` GASC left join (select * from (select ga_sourcemedium, final_channel, final_source, row_number() over (partition by lower(ga_sourcemedium) order by lower(ga_sourcemedium)) rw from maplemonk.TMS_GA_CHANNEL_MAPPING) where rw=1) GCM on lower(replace(GASC.sessionSourceMedium,\' \',\'\')) = lower(replace(GCM.ga_sourcemedium,\' \',\'\')) ;",
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
            