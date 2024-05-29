{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table if not exists BUMMER_DB.MAPLEMONK.courier_mapping (type varchar, courier varchar); create table if not exists BUMMER_DB.MAPLEMONK.zone_agreed_tat (type varchar, zone varchar, agreed_tat float); create or replace table BUMMER_DB.MAPLEMONK.BUMMER_DB_LOGISTICS_CONSOLIDATED as select SR.* ,zt.agreed_tat ,cm.type from (select * from (select distinct \'SHOPIFY_INDIA\' Marketplace , replace(A.Value:awb,\'\"\',\'\') awb ,zone ,is_return Return_Flag ,channel_order_id ,try_to_date(created_at,\'DD Mon YYYY, HH12:MI AM\') Created_date ,try_to_date(updated_at) Updated_date ,upper(status) status ,replace(A.Value:courier,\'\"\',\'\') Courier ,try_to_timestamp(picked_up_date) PICKEDUP_DATE ,try_to_timestamp(first_out_for_delivery_date) FIRST_OUT_FOR_DELIVERY_DATE ,try_to_timestamp(out_for_delivery_date, \'DD-MM-YYYY HH24:MI:SS\') OUT_FOR_DELIVERY_DATE ,try_to_timestamp(delivered_date , \'DD-MM-YYYY HH24:MI:SS\') DELIVERED_DATE ,\'SHIPROCKET\' as Shipment_Aggregator ,upper(payment_method) payment_method ,case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') end Return_Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end COD_CHARGES ,case when replace(awb_data:charges:freight_charges,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:freight_charges,\'\"\',\'\')- (case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end) end Forward_Shipping_Charges ,row_number() over (partition by replace(A.Value:awb,\'\"\',\'\') order by try_to_date(updated_at,\'DD Mon YYYY, HH12:MI AM\') desc) rw from BUMMER_DB.MAPLEMONK.SHIPROCKET_SHIPROCKET_BUMMER_ORDERS SR, lateral flatten (SHIPMENTS)A ) where rw = 1 ) SR left join BUMMER_DB.MAPLEMONK.courier_mapping cm on lower(SR.Courier) = lower(cm.courier) left join BUMMER_DB.MAPLEMONK.zone_agreed_tat zt on lower(cm.type) = lower(zt.type) and lower(SR.zone) = lower(zt.zone) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BUMMER_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        