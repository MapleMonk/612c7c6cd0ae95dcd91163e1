{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Campussutra_GA_Sessions_Consolidated AS SELECT Shop_Name, Channel, Final_Source, DATE, sessions, totalusers, property_id, engagedsessions, screenpageviews, sessionsourcemedium, averagesessionduration, screenpageviewspersession FROM MAPLEMONK.Campussutra_CS_GA_Sessions_Consolidated UNION ALL SELECT Shop_Name, Channel, Final_Source, DATE, sessions, totalusers, property_id, engagedsessions, screenpageviews, sessionsourcemedium, averagesessionduration, screenpageviewspersession FROM MAPLEMONK.Campussutra_FA_GA_Sessions_Consolidated UNION ALL SELECT Shop_Name, Channel, Final_Source, DATE, sessions, totalusers, property_id, engagedsessions, screenpageviews, sessionsourcemedium, averagesessionduration, screenpageviewspersession FROM MAPLEMONK.Campussutra_HS_GA_Sessions_Consolidated UNION ALL SELECT Shop_Name, Channel, Final_Source, DATE, sessions, totalusers, property_id, engagedsessions, screenpageviews, sessionsourcemedium, averagesessionduration, screenpageviewspersession FROM MAPLEMONK.Campussutra_IFP_GA_Sessions_Consolidated UNION ALL SELECT Shop_Name, Channel, Final_Source, DATE, sessions, totalusers, property_id, engagedsessions, screenpageviews, sessionsourcemedium, averagesessionduration, screenpageviewspersession FROM MAPLEMONK.Campussutra_SOHI_GA_Sessions_Consolidated;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            