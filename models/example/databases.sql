{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table xyxx_db.maplemonk.logistics_fact_items_xyxx as select distinct replace(A.Value:awb,\'\"\',\'\') awb ,try_to_date(created_at,\'DD Mon YYYY, HH24:MI AM\') Created_date ,try_to_date(updated_at,\'DD Mon YYYY, HH24:MI AM\') Updated_date ,status ,replace(A.Value:courier,\'\"\',\'\') Courier ,\'Shiprocket\' as Shipment_Aggregator ,case when replace(awb_data:charges:applied_weight_amount,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:applied_weight_amount,\'\"\',\'\') end Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:cod_charges,\'\"\',\'\') end COD_CHARGES from xyxx_db.maplemonk.shiprocket_orders, lateral flatten (SHIPMENTS)A union all select distinct AWB_NO AWB ,try_to_date(a.order_Date) Created_date ,try_to_date(a.last_scan_datetime) Updated_date ,a.latest_courier_status Status ,a.logistic as Courier ,\'iThink\' Shipment_Aggregator ,b.shipping_charges ,b.cod_charges from xyxx_db.maplemonk.ithink_get_orders a left join xyxx_db.maplemonk.mapping_shipment_cost b on b.from_date::date <= try_to_date(a.order_Date) and b.to_date::date >=try_to_date(a.order_Date);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        