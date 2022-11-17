{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate AS select \'Shopify_DRV\' as Shop_Name, * from RPSG_DB.MAPLEMONK.ga_drv_orders_by_source union all select \'Shopify_Herbobuild\' as Shop_Name, * from RPSG_DB.MAPLEMONK.ga_herbobuild_orders_by_source union all select \'Shopify_AyurvedicSource\' as Shop_Name, * from RPSG_DB.MAPLEMONK.ga_ayurvedic_orders_by_source; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_ORDER_BY_SOURCE_CONSOLIDATED_DRV AS select (case when GCM.final_channel is null then \'Others\' else GCM.final_channel end) Channel,GASC.* from RPSG_DB.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate GASC left join RPSG_DB.MAPLEMONK.GA_CHANNEL_MAPPING GCM on GASC.GA_SOURCEMEDIUM = GCM.GA_SOURCEMEDIUM; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_Sessions_Consolidated_Intermediate AS select \'Shopify_DRV\' as Shop_Name, _AIRBYTE_UNIQUE_KEY, GA_DATE, VIEW_ID, GA_EXITS, GA_USERS, GA_MEDIUM, GA_SOURCE, GA_CAMPAIGN, GA_NEWUSERS, GA_SESSIONS, GA_PAGEVIEWS, GA_SOURCEMEDIUM, GA_UNIQUEPAGEVIEWS, GA_AVGSESSIONDURATION, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_GA_DRV_SESSIONS_BY_DATE_HASHID from RPSG_DB.MAPLEMONK.ga_drv_sessions_by_date where ga_campaign is not null union all select \'Shopify_Herbobuild\' as Shop_Name, _AIRBYTE_UNIQUE_KEY, GA_DATE, VIEW_ID, GA_EXITS, GA_USERS, GA_MEDIUM, GA_SOURCE, GA_CAMPAIGN, GA_NEWUSERS, GA_SESSIONS, GA_PAGEVIEWS, GA_SOURCEMEDIUM, GA_UNIQUEPAGEVIEWS, GA_AVGSESSIONDURATION, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_GA_HERBOBUILD_SESSIONS_BY_DATE_HASHID from RPSG_DB.MAPLEMONK.ga_herbobuild_sessions_by_date where ga_campaign is not null union all select \'Shopify_AyurvedicSource\' as Shop_Name, _AIRBYTE_UNIQUE_KEY, GA_DATE, VIEW_ID, GA_EXITS, GA_USERS, GA_MEDIUM, GA_SOURCE, GA_CAMPAIGN, GA_NEWUSERS, GA_SESSIONS, GA_PAGEVIEWS, GA_SOURCEMEDIUM, GA_UNIQUEPAGEVIEWS, GA_AVGSESSIONDURATION, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_GA_AYURVEDIC_SESSIONS_BY_DATE_HASHID from RPSG_DB.MAPLEMONK.ga_ayurvedic_sessions_by_date where ga_campaign is not null; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_Sessions_Consolidated_DRV AS select (case when GCM.final_channel is null then \'Others\' else GCM.final_channel end) Channel,GASC.* from RPSG_DB.MAPLEMONK.GA_Sessions_Consolidated_Intermediate GASC left join RPSG_DB.MAPLEMONK.GA_CHANNEL_MAPPING GCM on GASC.GA_SOURCEMEDIUM = GCM.GA_SOURCEMEDIUM;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        