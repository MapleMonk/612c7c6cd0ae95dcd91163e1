{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE nessa_DB.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate AS select \'Shopify_Nessa\' as Shop_Name, * from nessa_DB.MAPLEMONK.ga_source__by__order; CREATE OR REPLACE TABLE nessa_DB.MAPLEMONK.GA_ORDER_BY_SOURCE_CONSOLIDATED_nessa AS select (case when GCM.final_channel is null then \'Others\' else GCM.final_channel end) Channel,GCM.FINAL_MEDIUM,GASC.* from nessa_DB.MAPLEMONK.GA_Order_By_Source_Consolidated_Intermediate GASC left join nessa_DB.MAPLEMONK.GA_CHANNEL_MAPPING GCM on GASC.GA_SOURCEMEDIUM = GCM.GA_SOURCEMEDIUM;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from NESSA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        