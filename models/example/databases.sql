{{ config(
            materialized='table',
                post_hook={
                    "sql": "select cast(timestampadd(minute, 330, db.beat_date) as date) beat_date,db.id, sum(sd.product_sold_quantity*pp.sku_count) beat_sold,ww.name, from my_sql_distributionchain_beatassignment db left join my_sql_warehouse_warehouse ww on db.warehouse_id = ww.id left join my_sql_saleschain_salesdemandsku sd on sd.beatAssignment_id = db.id left join my_sql_product_product pp on sd.product_id = pp.id where db.beat_status in(\'Ongoing\', \'Completed\') group by db.beat_date,db.id,ww.name) db join (select cast(timestampadd(minute, 330, oo.delivery_date) as date) order_delivery_date,cast(timestampadd(minute, 330, oo.generation_date) as date) order_generation_date ,oo.beat_assignment_id,db.beat_name,db.warehouse_id,ww.name, sum(ol.quantity*pp.sku_count) order_eggs, from my_sql_order_order oo left join my_sql_distributionchain_beatassignment db on oo.beat_assignment_id= db.id left join my_sql_warehouse_warehouse ww on db.warehouse_id = ww.id left join my_sql_order_orderline ol on oo.id= ol.order_id left join my_sql_product_product pp on ol.product_id = pp.id where oo.status in (\'delivered\',\'completed\') group by cast(timestampadd(minute, 330, oo.delivery_date) as date),cast(timestampadd(minute, 330, oo.generation_date) as date) , oo.beat_assignment_id,db.beat_name,db.warehouse_id,ww.name ) ps on db.id = ps.beat_assignment_id where ps.order_delivery_date like \'%2025-09%\' and coalesce (db.name,ps.name)=\'Choudhary Farm\' --and db.beat_date <> ps.order_delivery_date group by db.beat_date,ps.beat_name,db.beat_sold,ps.order_generation_date, ps.order_delivery_date,ps.order_eggs,db.name,ps.name",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            