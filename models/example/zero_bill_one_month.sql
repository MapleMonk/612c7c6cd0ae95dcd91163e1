{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "alter table eggozdb.maplemonk.zero_bill_one_month alter column cast(last_order_date as date); alter table eggozdb.maplemonk.zero_bill_one_month alter column cast(zb.onboarding_date as date);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.zero_bill_one_month
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        