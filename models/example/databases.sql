{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.maplemonk.bsc_db_GA_landing_page_funnel_shopify as select to_date(date, \'yyyymmdd\') Date ,SESSIONS ,CHECKOUTS ,ADDTOCARTS ,LANDINGPAGE ,PROPERTY_ID ,TRANSACTIONS ,SESSIONMEDIUM ,SESSIONSOURCE ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM ,upper(coalesce(GCM.final_channel, \'unmapped\' )) Channel ,upper(coalesce(GCM.final_source, \'unmapped\')) Final_Source from bsc_db.MAPLEMONK.google_analytics_4__ga4__ga4_bsc_landing_page_funnel_metrics GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, final_source, type,row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from bsc_db.Maplemonk.ga_channel_mapping) where rw=1) GCM on lower(GASC.SESSIONSOURCEMEDIUM) = lower(GCM.GA_SOURCEMEDIUM) ; create or replace table bsc_db.maplemonk.bsc_db_GA_marketing_channel_funnel_shopify as select to_date(date, \'yyyymmdd\') Date ,SESSIONS ,CHECKOUTS ,ADDTOCARTS ,PROPERTY_ID ,TRANSACTIONS ,SESSIONMEDIUM ,SESSIONSOURCE ,ENGAGEDSESSIONS ,SCREENPAGEVIEWS ,SESSIONSOURCEMEDIUM ,upper(coalesce(GCM.final_channel, \'unmapped\' )) Channel ,upper(coalesce(GCM.final_source, \'unmapped\')) Final_Source from bsc_db.MAPLEMONK.google_analytics_4__ga4__ga4_bsc_marketing_channel_funnel_metrics GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, final_source, row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from bsc_db.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.SESSIONSOURCEMEDIUM) = lower(GCM.GA_SOURCEMEDIUM) ;",
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
                        