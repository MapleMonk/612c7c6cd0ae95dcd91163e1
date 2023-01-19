{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.GA_Order_By_Source_Consolidated_Intermediate AS select \'Shopify_perfora_care\' as Shop_Name, * from Perfora_DB.MapleMonk.ga__source__by__order; CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.GA_ORDER_BY_SOURCE_CONSOLIDATED_Perfora AS select (case when GCM.final_channel is null then case when lower(GASC.GA_SOURCEMEDIUM) like \'%google%\' then \'Google\' when lower(GASC.GA_SOURCEMEDIUM) like \'%facebook%\' then \'Facebook\' when lower(GASC.GA_SOURCEMEDIUM) like \'%instagram%\' then \'Instagram\' when lower(GASC.GA_SOURCEMEDIUM) like \'%direct%\' then \'Direct\' when lower(GASC.GA_SOURCEMEDIUM) like \'%webengage%\' then \'Webengage\' when lower(GASC.GA_SOURCEMEDIUM) like \'%chatbot%\' then \'Chatbot\' when lower(GASC.GA_SOURCEMEDIUM) like \'%email%\' then \'Email\' when lower(GASC.GA_SOURCEMEDIUM) like \'%cashfree%\' then \'Cashfree\' when lower(GASC.GA_SOURCEMEDIUM) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.GA_SOURCEMEDIUM) like \'%youtube%\' then \'Youtube\' when lower(GASC.GA_SOURCEMEDIUM) like \'%duckduckgo%\' then \'DuckDuckGo\' when lower(GASC.GA_SOURCEMEDIUM) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.GA_SOURCEMEDIUM) like \'%bing%\' then \'Bing\' when lower(GASC.GA_SOURCEMEDIUM) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.GA_SOURCEMEDIUM) like \'%paytm%\' then \'Paytm\' when lower(GASC.GA_SOURCEMEDIUM) like \'%grabon%\' then \'Grabon\' when lower(GASC.GA_SOURCEMEDIUM) like \'%sms%\' then \'SMS\' when lower(GASC.GA_SOURCEMEDIUM) like \'%yahoo%\' then \'Yahoo\' when lower(GASC.GA_SOURCEMEDIUM) like \'%referral%\' then \'Referral\' else \'Others\' end else GCM.final_channel end) Channel,GASC.* from Perfora_DB.MapleMonk.GA_Order_By_Source_Consolidated_Intermediate GASC left join (select * from (select GA_SOURCEMEDIUM, final_channel, row_number() over (partition by lower(GA_SOURCEMEDIUM) order by lower(GA_SOURCEMEDIUM)) rw from Perfora_DB.MapleMonk.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.GA_SOURCEMEDIUM) = lower(GCM.GA_SOURCEMEDIUM); CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.GA_Sessions_Consolidated_Intermediate AS select \'Shopify_perfora_care\' as Shop_Name, * from Perfora_DB.MapleMonk.ga__session_by__date; CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.GA_Sessions_Consolidated_Perfora AS select (case when GCM.final_channel is null then case when lower(GASC.GA_SOURCE) like \'%google%\' then \'Google\' when lower(GASC.GA_SOURCE) like \'%facebook%\' then \'Facebook\' when lower(GASC.GA_SOURCE) like \'%instagram%\' then \'Instagram\' when lower(GASC.GA_SOURCE) like \'%direct%\' then \'Direct\' when lower(GASC.GA_SOURCE) like \'%webengage%\' then \'Webengage\' when lower(GASC.GA_SOURCE) like \'%chatbot%\' then \'Chatbot\' when lower(GASC.GA_SOURCE) like \'%email%\' then \'Email\' when lower(GASC.GA_SOURCE) like \'%cashfree%\' then \'Cashfree\' when lower(GASC.GA_SOURCE) like \'%affiliate%\' then \'Affiliate\' when lower(GASC.GA_SOURCE) like \'%youtube%\' then \'Youtube\' when lower(GASC.GA_SOURCE) like \'%duckduckgo%\' then \'DuckDuckGo\' when lower(GASC.GA_SOURCE) like \'%linkedin%\' then \'LinkedIn\' when lower(GASC.GA_SOURCE) like \'%bing%\' then \'Bing\' when lower(GASC.GA_SOURCE) like \'%whatsapp%\' then \'Whatsapp\' when lower(GASC.GA_SOURCE) like \'%paytm%\' then \'Paytm\' when lower(GASC.GA_SOURCE) like \'%grabon%\' then \'Grabon\' when lower(GASC.GA_SOURCE) like \'%sms%\' then \'SMS\' when lower(GASC.GA_SOURCE) like \'%yahoo%\' then \'Yahoo\' when lower(GASC.GA_SOURCE) like \'%referral%\' then \'Referral\' else \'Others\' end else GCM.final_channel end) Channel,GASC.* from Perfora_DB.MAPLEMONK.GA_Sessions_Consolidated_Intermediate GASC left join (select * from (select GA_SOURCE, final_channel, row_number() over (partition by lower(GA_SOURCE) order by lower(GA_SOURCE)) rw from Perfora_DB.MapleMonk.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(GASC.GA_SOURCE) = lower(GCM.GA_SOURCE);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        