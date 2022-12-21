{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.GA_Orders_Consolidated_Vahdam as select \'Shopify_USA\' as Region ,ga_date ,view_id ,ga_medium ,ga_source ,ga_campaign ,ga_sourcemedium ,ga_transactionid ,GA_REVENUEPERTRANSACTION ,case when lower(GA_SOURCEMEDIUM) like any (\'%facebook%\',\'%fb%\') then \'Facebook\' when lower(GA_SOURCEMEDIUM) like any (\'%instagram%\',\'%igshopping%\',\'%insta%\',\'%ig%\') then \'Instagram\' when lower(GA_SOURCEMEDIUM) like any (\'%google%\',\'%youtube%\') then \'Google\' when lower(GA_SOURCEMEDIUM) like \'%duckduckgo%\' then \'Duckduckgo\' when lower(GA_SOURCEMEDIUM) like \'%bing%\' then \'Bing\' when lower(GA_SOURCEMEDIUM) like \'%yahoo%\' then \'Yahoo\' when lower(GA_SOURCEMEDIUM) like \'%direct%\' then \'Direct\' when lower(GA_SOURCEMEDIUM) like \'%attentive%\' then \'Attentive\' when lower(GA_SOURCEMEDIUM) like \'%pushowl%\' then \'PushOwl\' when lower(GA_SOURCEMEDIUM) like any (\'%email%\',\'%klaviyo%\') then \'Email\' when lower(GA_SOURCEMEDIUM) like any (\'%affiliate%\',\'%affliate%\') then \'Affiliate\' when lower(GA_SOURCEMEDIUM) like \'%content%\' then \'Content\' when lower(GA_SOURCEMEDIUM) like \'%referral%\' then \'Referral\' when lower(GA_SOURCEMEDIUM) like \'%tiktok%\' then \'TikTok\' when lower(GA_SOURCEMEDIUM) like \'%sms%\' then \'SMS\' when lower(GA_SOURCEMEDIUM) like \'%telegram%\' then \'Telegram\' when lower(GA_SOURCEMEDIUM) like \'%twitter%\' then \'Twitter\' when lower(GA_SOURCEMEDIUM) like \'%pinterest%\' then \'Pinterest\' when lower(GA_SOURCEMEDIUM) like \'%linkedin%\' then \'LinkedIn\' else \'Others\' end as GA_Mapped_Channel from vahdam_db.maplemonk.ga_us_website_orders_by_source union all select \'Shopify_Germany\' as Region ,ga_date ,view_id ,ga_medium ,ga_source ,ga_campaign ,ga_sourcemedium ,ga_transactionid ,GA_REVENUEPERTRANSACTION ,case when lower(GA_SOURCEMEDIUM) like any (\'%facebook%\',\'%fb%\') then \'Facebook\' when lower(GA_SOURCEMEDIUM) like any (\'%instagram%\',\'%igshopping%\',\'%insta%\',\'%ig%\') then \'Instagram\' when lower(GA_SOURCEMEDIUM) like any (\'%google%\',\'%youtube%\') then \'Google\' when lower(GA_SOURCEMEDIUM) like \'%duckduckgo%\' then \'Duckduckgo\' when lower(GA_SOURCEMEDIUM) like \'%bing%\' then \'Bing\' when lower(GA_SOURCEMEDIUM) like \'%yahoo%\' then \'Yahoo\' when lower(GA_SOURCEMEDIUM) like \'%direct%\' then \'Direct\' when lower(GA_SOURCEMEDIUM) like \'%attentive%\' then \'Attentive\' when lower(GA_SOURCEMEDIUM) like \'%pushowl%\' then \'PushOwl\' when lower(GA_SOURCEMEDIUM) like any (\'%email%\',\'%klaviyo%\') then \'Email\' when lower(GA_SOURCEMEDIUM) like any (\'%affiliate%\',\'%affliate%\') then \'Affiliate\' when lower(GA_SOURCEMEDIUM) like \'%content%\' then \'Content\' when lower(GA_SOURCEMEDIUM) like \'%referral%\' then \'Referral\' when lower(GA_SOURCEMEDIUM) like \'%tiktok%\' then \'TikTok\' when lower(GA_SOURCEMEDIUM) like \'%sms%\' then \'SMS\' when lower(GA_SOURCEMEDIUM) like \'%telegram%\' then \'Telegram\' when lower(GA_SOURCEMEDIUM) like \'%twitter%\' then \'Twitter\' when lower(GA_SOURCEMEDIUM) like \'%pinterest%\' then \'Pinterest\' when lower(GA_SOURCEMEDIUM) like \'%linkedin%\' then \'LinkedIn\' else \'Others\' end as GA_Mapped_Channel from vahdam_db.maplemonk.ga_de_website_orders_by_source union all select \'Shopify_Global\' as Region ,ga_date ,view_id ,ga_medium ,ga_source ,ga_campaign ,ga_sourcemedium ,ga_transactionid ,GA_REVENUEPERTRANSACTION ,case when lower(GA_SOURCEMEDIUM) like any (\'%facebook%\',\'%fb%\') then \'Facebook\' when lower(GA_SOURCEMEDIUM) like any (\'%instagram%\',\'%igshopping%\',\'%insta%\',\'%ig%\') then \'Instagram\' when lower(GA_SOURCEMEDIUM) like any (\'%google%\',\'%youtube%\') then \'Google\' when lower(GA_SOURCEMEDIUM) like \'%duckduckgo%\' then \'Duckduckgo\' when lower(GA_SOURCEMEDIUM) like \'%bing%\' then \'Bing\' when lower(GA_SOURCEMEDIUM) like \'%yahoo%\' then \'Yahoo\' when lower(GA_SOURCEMEDIUM) like \'%direct%\' then \'Direct\' when lower(GA_SOURCEMEDIUM) like \'%attentive%\' then \'Attentive\' when lower(GA_SOURCEMEDIUM) like \'%pushowl%\' then \'PushOwl\' when lower(GA_SOURCEMEDIUM) like any (\'%email%\',\'%klaviyo%\') then \'Email\' when lower(GA_SOURCEMEDIUM) like any (\'%affiliate%\',\'%affliate%\') then \'Affiliate\' when lower(GA_SOURCEMEDIUM) like \'%content%\' then \'Content\' when lower(GA_SOURCEMEDIUM) like \'%referral%\' then \'Referral\' when lower(GA_SOURCEMEDIUM) like \'%tiktok%\' then \'TikTok\' when lower(GA_SOURCEMEDIUM) like \'%sms%\' then \'SMS\' when lower(GA_SOURCEMEDIUM) like \'%telegram%\' then \'Telegram\' when lower(GA_SOURCEMEDIUM) like \'%twitter%\' then \'Twitter\' when lower(GA_SOURCEMEDIUM) like \'%pinterest%\' then \'Pinterest\' when lower(GA_SOURCEMEDIUM) like \'%linkedin%\' then \'LinkedIn\' else \'Others\' end as GA_Mapped_Channel from vahdam_db.maplemonk.ga_global_website_orders_by_source union all select \'Shopify_India\' as Region ,ga_date ,view_id ,ga_medium ,ga_source ,ga_campaign ,ga_sourcemedium ,ga_transactionid ,GA_REVENUEPERTRANSACTION ,case when lower(GA_SOURCEMEDIUM) like any (\'%facebook%\',\'%fb%\') then \'Facebook\' when lower(GA_SOURCEMEDIUM) like any (\'%instagram%\',\'%igshopping%\',\'%insta%\',\'%ig%\') then \'Instagram\' when lower(GA_SOURCEMEDIUM) like any (\'%google%\',\'%youtube%\') then \'Google\' when lower(GA_SOURCEMEDIUM) like \'%duckduckgo%\' then \'Duckduckgo\' when lower(GA_SOURCEMEDIUM) like \'%bing%\' then \'Bing\' when lower(GA_SOURCEMEDIUM) like \'%yahoo%\' then \'Yahoo\' when lower(GA_SOURCEMEDIUM) like \'%direct%\' then \'Direct\' when lower(GA_SOURCEMEDIUM) like \'%attentive%\' then \'Attentive\' when lower(GA_SOURCEMEDIUM) like \'%pushowl%\' then \'PushOwl\' when lower(GA_SOURCEMEDIUM) like any (\'%email%\',\'%klaviyo%\') then \'Email\' when lower(GA_SOURCEMEDIUM) like any (\'%affiliate%\',\'%affliate%\') then \'Affiliate\' when lower(GA_SOURCEMEDIUM) like \'%content%\' then \'Content\' when lower(GA_SOURCEMEDIUM) like \'%referral%\' then \'Referral\' when lower(GA_SOURCEMEDIUM) like \'%tiktok%\' then \'TikTok\' when lower(GA_SOURCEMEDIUM) like \'%sms%\' then \'SMS\' when lower(GA_SOURCEMEDIUM) like \'%telegram%\' then \'Telegram\' when lower(GA_SOURCEMEDIUM) like \'%twitter%\' then \'Twitter\' when lower(GA_SOURCEMEDIUM) like \'%pinterest%\' then \'Pinterest\' when lower(GA_SOURCEMEDIUM) like \'%linkedin%\' then \'LinkedIn\' else \'Others\' end as GA_Mapped_Channel from vahdam_db.maplemonk.ga_in_website_orders_by_source union all select \'Shopify_Italy\' as Region ,ga_date ,view_id ,ga_medium ,ga_source ,ga_campaign ,ga_sourcemedium ,ga_transactionid ,GA_REVENUEPERTRANSACTION ,case when lower(GA_SOURCEMEDIUM) like any (\'%facebook%\',\'%fb%\') then \'Facebook\' when lower(GA_SOURCEMEDIUM) like any (\'%instagram%\',\'%igshopping%\',\'%insta%\',\'%ig%\') then \'Instagram\' when lower(GA_SOURCEMEDIUM) like any (\'%google%\',\'%youtube%\') then \'Google\' when lower(GA_SOURCEMEDIUM) like \'%duckduckgo%\' then \'Duckduckgo\' when lower(GA_SOURCEMEDIUM) like \'%bing%\' then \'Bing\' when lower(GA_SOURCEMEDIUM) like \'%yahoo%\' then \'Yahoo\' when lower(GA_SOURCEMEDIUM) like \'%direct%\' then \'Direct\' when lower(GA_SOURCEMEDIUM) like \'%attentive%\' then \'Attentive\' when lower(GA_SOURCEMEDIUM) like \'%pushowl%\' then \'PushOwl\' when lower(GA_SOURCEMEDIUM) like any (\'%email%\',\'%klaviyo%\') then \'Email\' when lower(GA_SOURCEMEDIUM) like any (\'%affiliate%\',\'%affliate%\') then \'Affiliate\' when lower(GA_SOURCEMEDIUM) like \'%content%\' then \'Content\' when lower(GA_SOURCEMEDIUM) like \'%referral%\' then \'Referral\' when lower(GA_SOURCEMEDIUM) like \'%tiktok%\' then \'TikTok\' when lower(GA_SOURCEMEDIUM) like \'%sms%\' then \'SMS\' when lower(GA_SOURCEMEDIUM) like \'%telegram%\' then \'Telegram\' when lower(GA_SOURCEMEDIUM) like \'%twitter%\' then \'Twitter\' when lower(GA_SOURCEMEDIUM) like \'%pinterest%\' then \'Pinterest\' when lower(GA_SOURCEMEDIUM) like \'%linkedin%\' then \'LinkedIn\' else \'Others\' end as GA_Mapped_Channel from vahdam_db.maplemonk.ga_it_website_orders_by_source;",
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
                        