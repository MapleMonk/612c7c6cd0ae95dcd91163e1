{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table opensecret_db.maplemonk.OS_LOGISTICS_FACT_ITEMS as select A.* , B.MAPPED_STATUS MAPPED_SHIPMENT_STATUS from opensecret_db.maplemonk.OS_shiprocket_fact_items A left join (select * from (select *, row_number() over (partition by lower(shipment_status) order by 1) rw from opensecret_db.maplemonk.shipment_status_mapping) where rw=1 ) B on lower(A.SHIPMENT_STATUS) = lower(B.shipment_status);",
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
                        