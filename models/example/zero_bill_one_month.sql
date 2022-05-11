{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.zero_bill_one_month ADD (order_DATE Date); UPDATE eggozdb.maplemonk.zero_bill_one_month SET order_date = TRY_TO_DATE(\"date(last_order_date)\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.zero_bill_one_month ADD (Onboarding_DATE Date); UPDATE eggozdb.maplemonk.zero_bill_one_month SET Onboarding_DATE = TRY_TO_DATE(\"date(zb.onboarding_date)\",\'DD/MM/YYYY\');",
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
                        