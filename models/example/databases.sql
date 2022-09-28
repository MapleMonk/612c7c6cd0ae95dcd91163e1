{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table ghc_db.maplemonk.Fact_Items_AmazonSellerPartner_ghc as SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-IST\" FROM ghc_db.maplemonk.asp_india_get_flat_file_all_orders_data_by_last_update_general WHERE upper(CURRENCY) = \'INR\' AND \"item-price\" NOT IN(\'\',\'0.0\');",
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
                        