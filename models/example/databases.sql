{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.non_saleable_inv as SELECT i.* FROM snitch_db.maplemonk.inventory_aging_buckets_snitch i LEFT JOIN snitch_db.maplemonk.ros_snitch r ON i.SKU_GROUP = r.SKU_GROUP WHERE r.SKU_GROUP IS NULL",
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
                        