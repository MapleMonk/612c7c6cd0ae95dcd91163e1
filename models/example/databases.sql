{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE HOX_DB.MAPLEMONK.HOX_DB_GA_ORDER_BY_SOURCE_CONSOLIDATED AS select \'ifeelblanko\' as Shop_Name ,upper(case when GCM.final_channel is null then case when lower(GASC.sourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sourceMedium) like \'%referral%\' then \'Referral\' else GASC.sourceMedium end else GCM.final_channel end) Channel ,upper(case when GCM.final_source is null then case when lower(GASC.sourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sourceMedium) like \'%referral%\' then \'Referral\' else GASC.sourceMedium end else GCM.final_source end) Final_Source ,to_date(GASC.date, \'yyyymmdd\') date ,GASC.medium ,GASC.source ,GASC.sourcemedium ,GASC.transactionid ,GASC.grosspurchaserevenue from HOX_DB.MAPLEMONK.GA4_GA4_BLANKO_HOX_ORDERS_BY_SOURCE GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, final_source, row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from HOX_DB.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.sourceMedium) = lower(GCM.GA_SOURCEMEDIUM) ; CREATE OR REPLACE TABLE HOX_DB.MAPLEMONK.HOX_DB_GA_Sessions_Consolidated AS select \'ifeelblanko\' as Shop_Name ,upper(case when GCM.final_channel is null then case when lower(GASC.sessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%referral%\' then \'Referral\' else \'Others\' end else GCM.final_channel end) Channel ,upper(case when GCM.final_channel is null then case when lower(GASC.sessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%referral%\' then \'Referral\' else GASC.sessionSourceMedium end else GCM.final_source end) Final_Source ,to_Date(GASC.date, \'yyyymmdd\') date ,GASC.sessions ,GASC.totalusers ,GASC.property_id ,GASC.engagedsessions ,GASC.screenpageviews ,GASC.sessionsourcemedium ,GASC.averagesessionduration ,GASC.screenpageviewspersession from HOX_DB.MAPLEMONK.GA4_GA4_BLANKO_HOX_SESSIONS_USERS_BY_DATE GASC left join (select * from (select ga_source, final_channel, final_source, row_number() over (partition by lower(ga_source) order by lower(ga_source)) rw from HOX_DB.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.sessionSourceMedium) = lower(GCM.ga_source) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        