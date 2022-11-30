{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table gynoveda_db.maplemonk.asp_fact_items_gynoveda as SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-IST\" FROM gynoveda_db.maplemonk.ASP_IN_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL WHERE upper(CURRENCY) = \'INR\' AND \"item-price\" NOT IN(\'\',\'0.0\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GYNOVEDA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        