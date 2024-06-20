{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.inventory_time_series as ( select current_date as date, sku_group, price, category, sku_class, xs_units, s_units, m_units, l_units, xl_units, xxl_units, xl3_units, xl4_units, xl5_units, xl6_units, available_units as inventory from snitch_db.maplemonk.availability_master );",
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
                        