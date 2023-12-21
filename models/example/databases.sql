{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table OFFDUTY_DB.maplemonk.OFFDUTY_DB_LOGISTICS_CONSOLIDATED as select sr.* ,ShipMap.final_shipping_status from (select distinct \'SHOPIFY_OFFDUTYSTORE\' Marketplace , replace(A.Value:awb,\'\"\',\'\') awb ,zone ,is_return Return_Flag ,channel_order_id ,try_to_date(created_at,\'DD Mon YYYY, HH12:MI AM\') Created_date ,try_to_date(updated_at) Updated_date ,status ,replace(A.Value:courier,\'\"\',\'\') Courier ,try_to_timestamp(picked_up_date) PICKEDUP_DATE ,try_to_timestamp(first_out_for_delivery_date) FIRST_OUT_FOR_DELIVERY_DATE ,try_to_timestamp(out_for_delivery_date, \'DD-MM-YYYY HH24:MI:SS\') OUT_FOR_DELIVERY_DATE ,try_to_timestamp(delivered_date , \'DD-MM-YYYY HH24:MI:SS\') DELIVERED_DATE ,\'Shiprocket\' as Shipment_Aggregator ,upper(payment_method) payment_method ,case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') end Return_Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end COD_CHARGES ,case when replace(awb_data:charges:freight_charges,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:freight_charges,\'\"\',\'\')- (case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end) end Forward_Shipping_Charges ,row_number() over (partition by replace(A.Value:awb,\'\"\',\'\') order by try_to_date(updated_at,\'DD Mon YYYY, HH12:MI AM\') desc) rw from OFFDUTY_DB.maplemonk.OFFDUTY_SHIPROCKET_ORDERS SR, lateral flatten (SHIPMENTS)A ) sr left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from offduty_db.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(sr.status) = lower(ShipMap.shipping_status) where sr.rw = 1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from offduty_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        