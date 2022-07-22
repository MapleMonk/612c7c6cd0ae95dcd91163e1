{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE maplemonk.region_wise_procurement_master_data ADD (GRN_date Date); UPDATE maplemonk.region_wise_procurement_master_data SET GRN_date = TRY_TO_DATE(\"GRN Date\",\'DD/MM/YYYY\');",
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
                        