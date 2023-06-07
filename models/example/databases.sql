{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table gladful_db.maplemonk.logistics_fact_items_gladful as select distinct \'Shopify_Gladful\' Marketplace ,replace(A.Value:awb,\'\"\',\'\') awb ,try_to_date(created_at,\'DD Mon YYYY, HH24:MI AM\') Created_date ,try_to_date(updated_at,\'DD Mon YYYY, HH24:MI AM\') Updated_date ,status ,replace(A.Value:courier,\'\"\',\'\') Courier ,\'Shiprocket\' as Shipment_Aggregator ,case when replace(awb_data:charges:applied_weight_amount,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:applied_weight_amount,\'\"\',\'\') end Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then NULL else replace(awb_data:charges:cod_charges,\'\"\',\'\') end COD_CHARGES from gladful_db.maplemonk.GALDFUL_SHIPROCKET_ORDERS, lateral flatten (SHIPMENTS)A;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GLADFUL_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        