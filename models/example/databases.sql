{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE IF NOT EXISTS BUMMER_DB.MAPLEMONK.GA_CHANNEL_MAPPING ( ga_sourcemedium VARCHAR(16777216), final_channel VARCHAR(16777216), final_source VARCHAR(16777216)); CREATE OR REPLACE TABLE BUMMER_DB.MAPLEMONK.BUMMER_DB_GA_ORDER_BY_SOURCE_CONSOLIDATED AS select \'Shopify_bummer_in\' as Shop_Name ,upper(case when GCM.final_channel is null then case when lower(GASC.SessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.SessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.SessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.SessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.SessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.SessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.SessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.SessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.SessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.SessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.SessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.SessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.SessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.SessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.SessionSourceMedium) like \'%referral%\' then \'Referral\' else GASC.SessionSourceMedium end else GCM.final_channel end) Channel ,upper(case when GCM.final_source is null then case when lower(GASC.SessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.SessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.SessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.SessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.SessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.SessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.SessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.SessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.SessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.SessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.SessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.SessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.SessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.SessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.SessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.SessionSourceMedium) like \'%referral%\' then \'Referral\' else GASC.SessionSourceMedium end else GCM.final_source end) Final_Source ,to_date(GASC.date, \'yyyymmdd\') date ,GASC.SessionMedium ,GASC.SessionSource ,GASC.SessionSourceMedium ,GASC.transactionid ,GASC.grosspurchaserevenue from BUMMER_DB.MAPLEMONK.GA4_GA4_Bummer_ORDERS_BY_SOURCE GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, final_source, row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from BUMMER_DB.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.SessionSourceMedium) = lower(GCM.GA_SOURCEMEDIUM) ; CREATE OR REPLACE TABLE BUMMER_DB.MAPLEMONK.BUMMER_DB_GA_Sessions_Consolidated AS select \'Shopify_bummer_in\' as Shop_Name ,upper(case when GCM.final_channel is null then case when lower(GASC.sessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%referral%\' then \'Referral\' else \'Others\' end else GCM.final_channel end) Channel ,upper(case when GCM.final_channel is null then case when lower(GASC.sessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%referral%\' then \'Referral\' else GASC.sessionSourceMedium end else GCM.final_source end) Final_Source ,to_Date(GASC.date, \'yyyymmdd\') date ,GASC.sessions ,GASC.totalusers ,GASC.property_id ,GASC.engagedsessions ,GASC.screenpageviews ,GASC.sessionsourcemedium ,GASC.averagesessionduration ,GASC.screenpageviewspersession from BUMMER_DB.MAPLEMONK.GA4_GA4_Bummer_SESSIONS_USERS_BY_DATE GASC left join (select * from (select ga_sourcemedium, final_channel, final_source, row_number() over (partition by lower(ga_sourcemedium) order by lower(ga_sourcemedium)) rw from BUMMER_DB.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.sessionSourceMedium) = lower(GCM.ga_sourcemedium) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BUMMER_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        