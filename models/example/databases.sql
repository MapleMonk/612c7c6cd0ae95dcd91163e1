{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from Test.MAPLEMONK.AMAZON_ADS_TEST_SPONSORED_BRANDS_REPORT_STREAM",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Test.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        