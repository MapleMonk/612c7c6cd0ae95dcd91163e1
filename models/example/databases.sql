{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Campussutra_GA_ORDER_BY_SOURCE_CONSOLIDATED AS SELECT Shop_Name, Channel, Final_Source, date, SessionMedium, SessionSource, SessionSourceMedium, transactionid, grosspurchaserevenue FROM MAPLEMONK.Campussutra_CS_GA_ORDER_BY_SOURCE_CONSOLIDATED UNION ALL SELECT Shop_Name, Channel, Final_Source, date, SessionMedium, SessionSource, SessionSourceMedium, transactionid, grosspurchaserevenue FROM MAPLEMONK.Campussutra_FA_GA_ORDER_BY_SOURCE_CONSOLIDATED UNION ALL SELECT Shop_Name, Channel, Final_Source, date, SessionMedium, SessionSource, SessionSourceMedium, transactionid, grosspurchaserevenue FROM MAPLEMONK.Campussutra_HS_GA_ORDER_BY_SOURCE_CONSOLIDATED UNION ALL SELECT Shop_Name, Channel, Final_Source, date, SessionMedium, SessionSource, SessionSourceMedium, transactionid, grosspurchaserevenue FROM MAPLEMONK.Campussutra_IFP_GA_ORDER_BY_SOURCE_CONSOLIDATED UNION ALL SELECT Shop_Name, Channel, Final_Source, date, SessionMedium, SessionSource, SessionSourceMedium, transactionid, grosspurchaserevenue FROM MAPLEMONK.Campussutra_SOHI_GA_ORDER_BY_SOURCE_CONSOLIDATED;",
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
            