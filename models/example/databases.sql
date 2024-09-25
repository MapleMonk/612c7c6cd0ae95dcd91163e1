{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table lagorii_db.maplemonk.lagorii_db_sale_rate_from_grn as select grn.*, inventory from (select m.sku, m.grn_date latest_grn_date, m.initial_quantity latest_grn_quantity, m.one_Week_sale, m.two_week_sale, sum(ifnull(n.quantity,0)) four_Week_sale from ( select z.sku, z.grn_Date, z.initial_quantity, z.one_week_sale, sum(ifnull(x.quantity,0)) two_week_sale from (select a.sku, a.grn_Date, a.quantity initial_quantity, sum(ifnull(b.quantity,0)) one_week_sale from (select * from ( select grn_id, grn_created_at::date grn_Date, A.value:sku::string sku, A.value:available quantity, A.value:grn_detail_price price, row_number() over (partition by sku order by grn_created_at::date desc) rw from lagorii_db.maplemonk.easyecom_lagorii_ee_grn_details, lateral flatten (INPUT => grn_items) A where A.value:available <> 0 ) where rw = 1 order by quantity desc ) a left join (select sku, order_date::date order_date, sum(ifnull(quantity,0)) quantity from lagorii_db.MAPLEMONK.lagorii_db_sales_consolidated group by 1,2 ) b on a.sku = b.sku and a.grn_date<= b.order_date and a.grn_date + 7 > b.order_date group by 1,2,3 ) z left join (select sku, order_date::date order_date, sum(ifnull(quantity,0)) quantity from lagorii_db.MAPLEMONK.lagorii_db_sales_consolidated group by 1,2 )x on z.sku = x.sku and z.grn_date<= x.order_date and z.grn_date + 14 > x.order_date group by 1,2,3,4 )m left join (select sku, order_date::date order_date, sum(ifnull(quantity,0)) quantity from lagorii_db.MAPLEMONK.lagorii_db_sales_consolidated group by 1,2 ) n on m.sku = n.sku and m.grn_date<= n.order_date and m.grn_date + 28 > n.order_date group by 1,2,3,4,5 ) grn left join (select \"Report Generated Date\"::date - 1 as date ,replace(sku,\'`\',\'\') sku, \"Available Quantity\" inventory from lagorii_db.maplemonk.easyecom_lagorii_ee_inventory_snapshot )inv_snap on grn.sku = inv_snap.sku and grn.latest_grn_Date = inv_snap.date ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from lagorii_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            