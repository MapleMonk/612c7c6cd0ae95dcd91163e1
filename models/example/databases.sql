{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Unicommerce_Inventory_Snapshot_Snitch_intermediate as select distinct ID ,name , brand , skucode , sum(opensale) OpenSale , sum(inventory) Inventory , date(timestamp) as DateOfInventory , categorycode , sum(openpurchase) OPENPURCHASE , sum(putawaypending) putawaypending , sum(vendorinventory) vendorinventory , sum(virtualinventory) virtualinventory , sum(pendingstocktransfer) pendingstocktransfer , sum(pendinginventoryassessment) pendinginventoryassessment from snitch_db.maplemonk.snitch_unicommerce_get_inventory_snapshot group by ID, name, brand, skucode, categorycode, date(timestamp) ; create or replace table snitch_db.maplemonk.Unicommerce_Inventory_Snapshot_Snitch as select Dateofinventory , Brand , Categorycode , skucode , Name , Inventory , ifnull(Qty_sold,0) Qty_Sold from snitch_db.maplemonk.Unicommerce_Inventory_Snapshot_Snitch_intermediate a left join ( select order_date ,sku , sum(suborder_quantity - return_quantity - cancelled_quantity) Qty_Sold from snitch_db.maplemonk.unicommerce_fact_items_snitch group by order_date, sku having order_date > \'2022-05-07\') b on a.skucode = b.sku and b.order_Date = a.dateofinventory where Qty_sold is not null",
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
                        