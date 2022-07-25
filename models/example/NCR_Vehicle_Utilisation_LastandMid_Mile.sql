{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.ncr_vehicle_utilisation_lastandmid_mile ADD (ddate date); UPDATE eggozdb.maplemonk.ncr_vehicle_utilisation_lastandmid_mile SET ddate = TRY_TO_DATE(\"Date\",\'DD/MM/YYYY\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.NCR_Vehicle_Utilisation_LastandMid_Mile
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        