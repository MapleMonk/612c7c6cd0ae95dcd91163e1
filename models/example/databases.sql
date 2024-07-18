{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.logicerp_stock_in_hand_current_date_data as select DATE::date as date ,LOT_MRP ,PACKING ,ITEM_MRP ,LOT_CODE ,ITEM_CF_1 ,ITEM_CF_2 ,ITEM_CF_3 ,ITEM_NAME ,PACK_NAME ,STOCK_QTY ,LOT_NUMBER ,SHADE_NAME ,BRANCH_CODE ,BRANCH_NAME ,GODOWN_CODE ,GODOWN_NAME ,LOT_SPRATE1 ,ADDLITEMCODE ,CARTON_STOCK ,try_to_date(LOT_PUR_DATE,\'DD/MM/YYYY\') as LOT_PUR_DATE ,LOGICUSERCODE ,LOT_SALE_RATE ,ITEM_SALE_RATE ,LOT_BASIC_RATE ,LOT_EXPIRY_DATE FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand where date::date = current_date()",
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
                        