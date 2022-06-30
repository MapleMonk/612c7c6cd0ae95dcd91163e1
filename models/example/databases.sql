{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table tcns_demo.maplemonk.mm_tcns_Latest_Inventory as select a.Storeid, a.ItemID, InvQty - ifnull(SaleQty,0) as Inv_Qty_Remaining, b.Date Sales_date, a.fetch_Date as Inv_date from (select storeid, itemid, sum(qty) InvQty, fetch_date from tcns_demo.public.inventory group by storeid,itemid,fetch_date) a left join (select Date, store, itemid, sum(qty) SaleQty from tcns_demo.public.sales group by store, itemid, Date) b on a.storeid = b.store and a.itemid = b.itemid and a.fetch_date = b.date",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from TCNS_DEMO.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        