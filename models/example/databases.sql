{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.gatepass_tracking AS ( SELECT gs.\"Order Placed Date\", gs.\"Gate-Pass Number\", gs.facility, TO_TIMESTAMP(gp.\"Gatepass Updated At\") AS \"Updated Timestamp\", gs.\"RTD Date (Ready to Dispacth)\", gs.\"Quantity Ordered\", SUM(gp.QUANTITY) AS QtyScanned, gs.\"Warehouse Status\", gp.\"To Party\" FROM snitch_db.maplemonk.google_sheet_store_goods_tracker_summary gs LEFT JOIN snitch_db.maplemonk.unicommerce_gatepass gp ON gs.\"Gate-Pass Number\" = gp.\"Gatepass Code\" Where gs.\"Warehouse Status\" <> \'Delivered\' GROUP BY gs.\"Order Placed Date\", gs.\"Gate-Pass Number\", gs.facility, \"Updated Timestamp\", gs.\"Quantity Ordered\", gp.\"To Party\", gs.\"Warehouse Status\", gs.\"RTD Date (Ready to Dispacth)\" )",
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
                        