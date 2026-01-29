{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.sku_live_mech_v2 as with data as ( select sku_group, sum(CASE WHEN transfer_branch_code != 32 then total_qty else null end) as ordered, sum( CASE WHEN transfer_branch_code = 8 then total_qty else null end) as south, sum( CASE WHEN transfer_branch_code = 94 then total_qty else null end) as north from SNITCH_DB.MAPLEMONK.LOGIC_PURCHASE_ORDER group by 1 ) , puta as ( select UPPER(TRIM(REGEXP_REPLACE(\"Item Type skuCode\"::STRING, \'-[^-]+$\', \'\'))) AS sku_group, sum( CASE WHEN \"Warehouse Name\" = \'SAPL-WH2\' then putaway_completed_quantity else null end ) as south , sum( CASE WHEN \"Warehouse Name\" = \'SAPL-NORTH-TAURU\' then putaway_completed_quantity else null end ) as north, sum( CASE WHEN \"Warehouse Name\" = \'SAPL-NORTH-TAURU\' or \"Warehouse Name\" = \'SAPL-WH2\' then putaway_completed_quantity else null end ) as inwarded, from snitch_db.maplemonk.putaway_tracking where final_type = \'New Inward\' group by 1 ) , final as (select sku_group, d.ordered, d.south South_PO, d.north North_PO, p.inwarded, p.south south_inwarded, p.north North_inwarded, (p.inwarded/nullif(d.ordered,0)) inwarded_prec, case when (p.inwarded/nullif(d.ordered,0))>=0.9 then \'Inwarded\' else \'Not Inwarded Yet\' end Remarks, case when d.south>0 then 1 else 0 end + case when d.north>0 then 1 else 0 end PO_raised_WH_count, case when p.south>0 then 1 else 0 end + case when p.north>0 then 1 else 0 end inwarded_WH_count, CASE WHEN b.final_live_date IS NULL AND ( marketplace_sales30 = 0 OR marketplace_sales30 IS NULL OR marketplace_sales30 = \'\' ) THEN \'Not Live yet\' WHEN b.final_live_date IS NOT NULL THEN \'Live on \' || TO_VARCHAR(b.final_live_date) ELSE \'Marketplace live\' END AS Live_Status_Remarks, case when c.sku_group is null then \'No Offline Allocation\' else \'Offline Order Raised\' end Offline_Ordered_Status from data d left join puta p using(sku_group) left join snitch_db.maplemonk.product_journey b using(sku_group) left join ( select distinct REGEXP_REPLACE(sku, \'-[^-]+$\', \'\') sku_group from snitch_db.maplemonk.b2b_journey ) c using(sku_group) where sku_group not in (select distinct sku_group from snitch_db.maplemonk.horizontal_sales_categories where sku_group is not null) and ordered>=100 ) select sku_group, ordered, South_PO, North_PO, inwarded, south_inwarded, North_inwarded, PO_raised_WH_count, inwarded_WH_count, round(100*inwarded_prec,1) inwarded_prec, case when sku_group like \'MP%\' then \'Marketplace Specific SKU\' when inwarded_prec>=0.9 and PO_raised_WH_count<=inwarded_WH_count then \'Majority Inwarded\' else \'Not Inwarded enough\' end Remarks, Live_Status_Remarks, Offline_Ordered_Status from final;",
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
            