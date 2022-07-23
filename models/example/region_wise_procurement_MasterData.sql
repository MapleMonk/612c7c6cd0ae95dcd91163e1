{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.region_wise_procurement_masterdata ADD (GRN_Date date); UPDATE eggozdb.maplemonk.region_wise_procurement_masterdata SET GRN_Date = TRY_TO_DATE(\"GRN Date\",\'DD/MM/YYYY\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.region_wise_procurement_MasterData
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        