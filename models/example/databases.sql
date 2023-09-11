{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace snitch_db.maplemonk.store_stock_aging as select branch_name,logicusercode, TO_DATE(replace(lot_pur_date,\'\"\',\'\'),\'DD/MM/YYYY\') as date_inward, sum(stock_qty) as total_qty from snitch_db.maplemonk.logicerp_get_stock_in_hand group by branch_name, logicusercode,date_inward order by date_inward",
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
                        