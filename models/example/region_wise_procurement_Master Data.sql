{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.region_wise_procurement_master_data ADD (GRN_date Date); UPDATE eggozdb.maplemonk.region_wise_procurement_master_data SET GRN_date = TRY_CONVERT(DATETIME,\"GRN_Date\");",
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
                        