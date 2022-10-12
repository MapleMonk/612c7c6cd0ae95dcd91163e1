{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE hilodesign_db.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate AS select \'Shopify_India\' as Shop_Name, * from hilodesign_db.MAPLEMONK.ga_orders_by_source; CREATE OR REPLACE TABLE hilodesign_db.MAPLEMONK.GA_ORDER_BY_SOURCE_CONSOLIDATED_HILO AS select GASC.* from hilodesign_db.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate GASC; CREATE OR REPLACE TABLE hilodesign_db.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate AS select \'Shopify_India\' as Shop_Name, * from hilodesign_db.MAPLEMONK.ga_orders_by_source CREATE OR REPLACE TABLE hilodesign_DB.MAPLEMONK.GA_traffic_Consolidated_Intermediate AS select \'Shopify_India\' as Shop_Name, * from hilodesign_DB.MAPLEMONK.ga_traffic_by_campaign where ga_campaign is not null; CREATE OR REPLACE TABLE hilodesign_DB.MAPLEMONK.GA_traffic_Consolidated_hilo AS select GASC.* from hilodesign_DB.MAPLEMONK.GA_traffic_Consolidated_Intermediate GASC;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HILODESIGN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        