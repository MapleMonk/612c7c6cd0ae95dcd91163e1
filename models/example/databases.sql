{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Unicommerce_Inventory_Snapshot_Snitch as select distinct ID ,name , brand , skucode , facility , opensale , inventory , date(timestamp) as DateOfInventory , categorycode , openpurchase , putawaypending , vendorinventory , virtualinventory , pendingstocktransfer , pendinginventoryassessment from snitch_db.maplemonk.snitch_unicommerce_get_inventory_snapshot",
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
                        