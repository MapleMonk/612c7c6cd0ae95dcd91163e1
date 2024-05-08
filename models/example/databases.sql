{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ttk_db.maplemonk.ttk_db_wareiq_fact_items as select awb, courier, status, last_updated last_updated_date, wareiq_account from ( select *, row_number() over (partition by awb order by last_updated desc ) rw from ( select shipping_details:awb::string awb, shipping_details:courier::string courier, status, last_updated, \'LD\' wareiq_account from ttk_db.maplemonk.wareiq_ld_orders where shipping_details:awb::string is not null union all select shipping_details:awb::string awb, shipping_details:courier::string courier, status, last_updated, \'SKORE\' wareiq_account from ttk_db.maplemonk.wareiq_skore_orders where shipping_details:awb::string is not null union all select shipping_details:awb::string awb, shipping_details:courier::string courier, status, last_updated, \'MKP\' wareiq_account from ttk_db.maplemonk.wareiq_mkp_orders where shipping_details:awb::string is not null union all select shipping_details:awb::string awb, shipping_details:courier::string courier, status, last_updated, \'MSC\' wareiq_account from ttk_db.maplemonk.wareiq_msc_orders where shipping_details:awb::string is not null )) where rw = 1",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ttk_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        