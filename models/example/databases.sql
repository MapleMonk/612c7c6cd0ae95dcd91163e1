{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table gynoveda_db.maplemonk.Latest_awb_Statuses as select distinct awb_no ,latest_courier_status ,last_scan_datetime ,\'iThink\' as shipment_partner from (select *, rank() over (partition by awb_no order by last_scan_datetime desc) rank from gynoveda_db.maplemonk.ithink_get_orders) where awb_no is not null and rank = 1 union all select distinct order_id awb_no ,replace(trackingevents[0]:status,\'\"\') latest_courier_status ,replace(trackingevents[0]:datetime,\'\"\') last_scan_datetime ,\'Vamaship\' as shipment_partner from gynoveda_db.maplemonk.vamaship_surface_tracking, lateral flatten (trackingevents) where order_id is not null union all select distinct replace(shipping_details:awb,\'\"\') awb_no ,status latest_courier_status ,last_updated last_scan_datetime ,\'WareIQ\' as shipment_partner from gynoveda_db.maplemonk.wareiq_orders where replace(shipping_details:awb,\'\"\') is not null and awb_no not in ( \'13624610352100\', \'13624610352435\', \'13624610350490\', \'13624610351864\', \'13624610351142\', \'70475941211\', \'13624610351805\', \'70475941126\', \'13624610351960\' ) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GYNOVEDA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        