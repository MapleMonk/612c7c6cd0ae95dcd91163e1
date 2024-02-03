{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.transport_costs_fm_vehicle_details_after_1st_april ADD (ddate Date); UPDATE eggozdb.maplemonk.transport_costs_fm_vehicle_details_after_1st_april SET ddate = TRY_TO_DATE(Date,\'DD/MM/YYYY\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.transport_costs_last mid mile
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        