{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.daily_store_alerts as select branch_name,sum(stock_qty) as units_on_hand from snitch_db.maplemonk.logicerp_get_stock_in_hand group by branch_name",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        