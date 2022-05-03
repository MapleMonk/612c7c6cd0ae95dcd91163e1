{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table tcns_db.test.mm_tcns_Latest_Inventory as select a.Storeid, a.ItemID, InvQty - ifnull(SaleQty,0) as Inv_Qty_Remaining, \'2022-04-27\' as Date from (select storeid, itemid, sum(qty) InvQty from tcns_db.test.mm_tcns_inventory group by storeid,itemid) a left join (select Date, store, itemid, sum(qty) SaleQty from tcns_db.test.mm_tcnssales where date = \'2022-04-27\' group by store, itemid, Date) b on a.storeid = b.store and a.itemid = b.itemid",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from TCNS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        