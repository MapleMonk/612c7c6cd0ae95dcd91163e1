{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table skinq_db.maplemonk.Skinq_logistics_fact_items as select distinct \'Shopify_skin_q\' Marketplace ,replace(A.Value:awb,\'\"\',\'\') awb ,try_to_date(created_at,\'DD Mon YYYY, HH12:MI AM\') Created_date ,try_to_date(updated_at) Updated_date ,status ,replace(A.Value:courier,\'\"\',\'\') Courier ,\'Shiprocket\' as Shipment_Aggregator ,case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') end Return_Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end COD_CHARGES ,case when replace(awb_data:charges:freight_charges,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:freight_charges,\'\"\',\'\')- (case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end) end Forward_Shipping_Charges from skinq_db.maplemonk.skinq_shiprocket_orders, lateral flatten (SHIPMENTS)A;",
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
                        