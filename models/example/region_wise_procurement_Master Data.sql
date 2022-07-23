{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.region_wise_procurement_master_data ADD GRN_Date date; UPDATE eggozdb.maplemonk.region_wise_procurement_master_data SET GRN_Date = TRY_TO_DATE(\"GRN Date\",\'YYYY/MM/DD\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.region_wise_procurement_Master Data
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        