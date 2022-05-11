{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "alter table eggozdb.maplemonk.zero_bill_one_month alter column MODIFY(last_order_date date); alter table eggozdb.maplemonk.zero_bill_one_month alter column MODIFY(zb.onboarding_datedate date);",
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
                        