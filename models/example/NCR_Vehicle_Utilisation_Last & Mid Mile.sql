{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.ncr_vehicle_utilisation_last___mid_mile ADD (Date date); UPDATE eggozdb.maplemonk.ncr_vehicle_utilisation_last___mid_mile SET Date = TRY_TO_DATE(\"Date\",\'DD/MM/YYYY\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.NCR_Vehicle_Utilisation_Last & Mid Mile
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        