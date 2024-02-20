{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE avorganics_db.MAPLEMONK.avorganics_DB_GA_Sessions_Consolidated AS select \'Shopify_drink_evocus\' as Shop_Name ,upper(case when GCM.final_channel is null then case when lower(GASC.sessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%referral%\' then \'Referral\' else \'Others\' end else GCM.final_channel end) Channel ,upper(case when GCM.final_channel is null then case when lower(GASC.sessionSourceMedium) like \'%google%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%facebook%\' then \'Facebook\' when lower(GASC.sessionSourceMedium) like \'%instagram%\' then \'Instagram\' when lower(GASC.sessionSourceMedium) like \'%direct%\' then \'Direct\' when lower(GASC.sessionSourceMedium) like \'%webengage%\' then \'Push Notification\' when lower(GASC.sessionSourceMedium) like \'%chatbot%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%email%\' then \'Email\' when lower(GASC.sessionSourceMedium) like \'%cashfree%\' then \'Payment Providers\' when lower(GASC.sessionSourceMedium) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%youtube%\' then \'Google\' when lower(GASC.sessionSourceMedium) like \'%duckduckgo%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.sessionSourceMedium) like \'%bing%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.sessionSourceMedium) like \'%paytm%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%grabon%\' then \'Affiliate\' when lower(GASC.sessionSourceMedium) like \'%sms%\' then \'SMS\' when lower(GASC.sessionSourceMedium) like \'%yahoo%\' then \'Organic\' when lower(GASC.sessionSourceMedium) like \'%referral%\' then \'Referral\' else GASC.sessionSourceMedium end else GCM.final_source end) Final_Source ,to_Date(GASC.date, \'yyyymmdd\') date ,GASC.sessions ,GASC.totalusers ,GASC.property_id ,GASC.engagedsessions ,GASC.checkouts ,GASC.addtocarts ,GASC.screenpageviews ,GASC.sessionsourcemedium ,GASC.averagesessionduration ,GASC.screenpageviewspersession from avorganics_db.MAPLEMONK.GA4_evocus_Sessions_by_date_source GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, final_source, row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from avorganics_db.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.sessionSourceMedium) = lower(GCM.GA_SOURCEMEDIUM) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from avorganics_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        