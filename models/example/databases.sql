{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table BSC_DB.MAPLEMONK.missing_utms as select distinct * from ( select distinct gokwik_utm_source Source, gokwik_utm_medium medium from BSC_DB.MAPLEMONK.Shopify_bombae_All_orders where gokwik_mapped_source = \'UNMAPPED\' union all select distinct gokwik_utm_source, gokwik_utm_medium from BSC_DB.MAPLEMONK.Shopify_All_orders where gokwik_mapped_source = \'UNMAPPED\' ) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        