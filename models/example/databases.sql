{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ghc_db.maplemonk.shopify_utm_campaigns_wise_orders as select count(ID) as No_of_Orders, LANDING_UTM_CAMPAIGN, created_at::date as Order_Date From GHC_DB.maplemonk.shopify_all_orders group by LANDING_UTM_CAMPAIGN, created_at::date",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GHC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        