{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table opensecret_db.maplemonk.OS_shiprocket_fact_items as select distinct replace(A.Value:awb,\'\"\',\'\') awb ,replace(A.Value:return_awb,\'\"\',\'\') return_awb ,replace(A.Value:return_awb,\'\"\',\'\') rto_awb ,try_to_date(created_at,\'DD Mon YYYY, HH24:MI AM\') Created_date ,try_to_date(updated_at,\'DD Mon YYYY, HH24:MI AM\') Updated_date ,upper(status) SHIPMENT_STATUS ,upper(pickup_location) PICKUP_LOCATION ,customer_pincode ,upper(replace(A.Value:courier,\'\"\',\'\')) Courier ,upper(replace(A.Value:delay_reason,\'\"\',\'\')) DELAY_REASON ,replace(A.Value:pickedup_timestamp,\'\"\',\'\') PICKEDUP_DATE ,replace(A.Value:shipped_date,\'\"\',\'\') SHIPPED_DATE ,replace(A.Value:delivered_date,\'\"\',\'\') DELIVERED_DATE ,replace(A.Value:rto_initiated_date,\'\"\',\'\') RTO_INTITATED_DATE ,replace(A.Value:rto_delivered_date,\'\"\',\'\') RTO_DELIVERED_DATE ,\'SHIPROCKET\' as Shipment_Aggregator ,case when replace(awb_data:charges:applied_weight_amount,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:applied_weight_amount,\'\"\',\'\') end Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:cod_charges,\'\"\',\'\') end COD_CHARGES ,case when replace(awb_data:charges:zone,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:zone,\'\"\',\'\') end ZONE ,payment_method ,is_return from opensecret_db.maplemonk.OS_Shiprocket_orders, lateral flatten (SHIPMENTS)A;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from opensecret_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        