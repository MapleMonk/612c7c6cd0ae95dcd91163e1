{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.region_wise_procurement_masterdata ADD (GRN_DATE Date); UPDATE eggozdb.maplemonk.region_wise_procurement_masterdata SET GRN_DATE = TRY_TO_DATE(\"GRN Date\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.region_wise_procurement_masterdata ADD (BILL_DATE Date); UPDATE eggozdb.maplemonk.region_wise_procurement_masterdata SET BILL_DATE = TRY_TO_DATE(\"Bill Date\",\'DD/MM/YYYY\'); UPDATE eggozdb.maplemonk.region_wise_procurement_masterdata SET region = \'East\' where lower(region) in (\'kolkata\',\'bihar\');",
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
                        