{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create Table ABC_ACQUISITION_SOURCE_ALL_DATA AS SELECT src.*, Call.\"Number Count\", Call.\"Avg Talk Time (mins)\", Call.\"Total Call Duration (hh mm ss)\" FROM SELECT_DB.MAPLEMONK.CLEANED_ABC_ABANDONED_CHECKOUTS AS src JOIN SELECT_DB.MAPLEMONK.ABC_ABNDED_CLEAN_DF1__3__CSV AS Call ON CAST(src.PHONE AS Int) = CAST(Call.CUSTOMER AS Int);",
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
                        