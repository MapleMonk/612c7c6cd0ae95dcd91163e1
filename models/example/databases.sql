{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table skinq_db.maplemonk.Skinq_logistics_fact_items as select distinct \'Shopify_skin_q\' Marketplace ,replace(A.Value:awb,\'\"\',\'\') awb ,try_to_date(created_at,\'DD Mon YYYY, HH24:MI AM\') Created_date ,try_to_date(updated_at,\'DD Mon YYYY, HH24:MI AM\') Updated_date ,status ,replace(A.Value:courier,\'\"\',\'\') Courier ,\'Shiprocket\' as Shipment_Aggregator ,case when replace(awb_data:charges:applied_weight_amount,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:applied_weight_amount,\'\"\',\'\') end Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:cod_charges,\'\"\',\'\') end COD_CHARGES from skinq_db.maplemonk.skinq_shiprocket_orders, lateral flatten (SHIPMENTS)A;",
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
                        