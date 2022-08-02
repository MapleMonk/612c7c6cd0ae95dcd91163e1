{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate AS select \'Shopify_DRV\' as Shop_Name, * from ga_drv_orders_by_source union all select \'Shopify_Herbobuild\' as Shop_Name, * from ga_herbobuild_orders_by_source union all select \'Shopify_Ayurvedic_Source\' as Shop_Name, * from ga_ayurvedic_orders_by_source; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_ORDER_BY_SOURCE_CONSOLIDATED_DRV AS select (case when GCM.final_channel is null then \'Others\' else GCM.final_channel end) Channel,GASC.* from RPSG_DB.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate GASC left join GA_CHANNEL_MAPPING GCM on GASC.GA_SOURCEMEDIUM = GCM.GA_SOURCEMEDIUM; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_Sessions_Consolidated_Intermediate AS select \'Shopify_DRV\' as Shop_Name, * from ga_drv_sessions_by_date union all select \'Shopify_Herbobuild\' as Shop_Name, * from ga_herbobuild_sessions_by_date union all select \'Shopify_Ayurvedic_Source\' as Shop_Name, * from ga_ayurvedic_sessions_by_date; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.GA_Sessions_Consolidated_DRV AS select (case when GCM.final_channel is null then \'Others\' else GCM.final_channel end) Channel,GASC.* from RPSG_DB.MAPLEMONK.GA_Sessions_Consolidated_Intermediate GASC left join GA_CHANNEL_MAPPING GCM on GASC.GA_SOURCEMEDIUM = GCM.GA_SOURCEMEDIUM;",
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
                        