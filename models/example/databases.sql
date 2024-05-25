{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE or replace table select_db.maplemonk.orm_data as select ASIN ,to_date(DATE,\'DD/MM/YYYY\' )::date as date ,REPLACE(SC_SALES, \',\', \'\')::int AS SC_SALES, ,SC_UNITS ,REPLACE(VC_SALES, \',\', \'\')::int AS VC_SALES, ,VC_UNITS ,DF_RATING ,DF_REVIEW ,CALL_COUNT ,\"(Child) ASIN\" ,\"Review Count\" ,\"Reviews: Rating\" ,\"Reviews: Review Count\" from select_db.maplemonk.select_db_orm_data",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        