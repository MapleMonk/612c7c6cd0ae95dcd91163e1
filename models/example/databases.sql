{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table MYMUSE_DB.maplemonk.MYMUSE_LOGISTICS_CONSOLIDATED as select * from (select distinct \'SHOPIFY_MYMUSE_INDIA\' Marketplace , replace(A.Value:awb,\'\"\',\'\') awb ,try_to_date(created_at,\'DD Mon YYYY, HH24:MI AM\') Created_date ,try_to_date(updated_at) Updated_date ,status ,replace(A.Value:courier,\'\"\',\'\') Courier ,\'Shiprocket\' as Shipment_Aggregator ,case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') end Return_Shipping_Charges ,case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end COD_CHARGES ,case when replace(awb_data:charges:freight_charges,\'\"\',\'\')=\'\' then 0 else replace(awb_data:charges:freight_charges,\'\"\',\'\')- (case when replace(awb_data:charges:cod_charges,\'\"\',\'\')=\'\' then 0 else (case when replace(awb_data:charges:charged_weight_amount_rto,\'\"\',\'\') >0 then 0 else replace(awb_data:charges:cod_charges,\'\"\',\'\') end) end) end Forward_Shipping_Charges ,row_number() over (partition by replace(A.Value:awb,\'\"\',\'\') order by try_to_date(updated_at,\'DD Mon YYYY, HH24:MI AM\') desc) rw from MYMUSE_DB.maplemonk.MYMUSE_shiprocket_orders, lateral flatten (SHIPMENTS)A ) where rw = 1 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MYMUSE_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        