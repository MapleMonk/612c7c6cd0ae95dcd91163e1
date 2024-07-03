{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table skinq_db.maplemonk.skinq_shiprocket_fact_items as select * from (select distinct \'SHOPIFY_SKIN_Q\' Marketplace ,zone , replace(A.Value:awb,\'\"\',\'\') awb ,is_return Return_Flag ,channel_order_id ,try_to_date(created_at,\'DD Mon YYYY, HH12:MI AM\') Created_date ,try_to_date(updated_at) Updated_date ,upper(status) status ,replace(A.Value:courier,\'\"\',\'\') Courier ,try_to_timestamp(picked_up_date) PICKEDUP_DATE ,try_to_timestamp(first_out_for_delivery_date) FIRST_OUT_FOR_DELIVERY_DATE ,try_to_timestamp(out_for_delivery_date, \'DD-MM-YYYY HH24:MI:SS\') OUT_FOR_DELIVERY_DATE ,try_to_timestamp(delivered_date , \'DD-MM-YYYY HH24:MI:SS\') DELIVERED_DATE ,\'SHIPROCKET\' as Shipment_Aggregator ,upper(payment_method) payment_method ,case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') end Return_Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end COD_CHARGES ,case when replace(awb_data:charges:freight_charges,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:freight_charges,\'\"\',\'\')- (case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end) end Forward_Shipping_Charges ,row_number() over (partition by replace(A.Value:awb,\'\"\',\'\') order by try_to_date(updated_at,\'DD Mon YYYY, HH12:MI AM\') desc) rw from SKINQ_DB.MAPLEMONK.SHIPROCKET_ORDERS SR, lateral flatten (SHIPMENTS)A ) where rw = 1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from skinq_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        