{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.ecoreturns_data as select replace(_airbyte_data:created_at,\'\"\',\'\')::timestamp created_at ,replace(_airbyte_data:credit_amount,\'\"\',\'\')::float credit_amount ,replace(_airbyte_data:name,\'\"\',\'\') name ,replace(_airbyte_data:order_id,\'\"\',\'\') ordeR_id ,replace(_airbyte_data:ran,\'\"\',\'\') ran ,replace(_airbyte_data:request_type,\'\"\',\'\') request_type ,replace(_airbyte_data:return_reasons,\'\"\',\'\') return_reasons ,replace(_airbyte_data:reverse_shipment_fee_paid,\'\"\',\'\')::float reverse_shipment_fee_paid from snitch_db.maplemonk._airbyte_raw_ecoreturns_historical union all select created_at ,credit_amount ,name ,ordeR_id ,ran ,request_type ,return_reasons ,reverse_shipment_fee_paid from snitch_db.maplemonk.ecoreturns_returns where ordeR_id not in (select distinct replace(_airbyte_data:order_id,\'\"\',\'\') from snitch_db.maplemonk._airbyte_raw_ecoreturns_historical) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        