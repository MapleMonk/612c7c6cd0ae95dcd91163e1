{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table ABC_ACQUISITION_SOURCE_ALL_DATA AS SELECT src.*, Call.\"Number Count\", Call.\"Avg Talk Time (mins)\", Call.\"Total Call Duration (hh mm ss)\", Wsap.\"TOTAL_MSG_SENT\", Wsap.\"PHNNUMBER\", Wsap. \"NUMBER\" FROM SELECT_DB.MAPLEMONK.CLEANED_ABC_ABANDONED_CHECKOUTS AS src JOIN SELECT_DB.MAPLEMONK.ABC_ABNDED_CLEAN_DF1__3__CSV AS Call ON CAST(src.PHONE AS INT) = CAST(Call.CUSTOMER AS INT) JOIN ABC_W_OUTPUT_FILE_CSV AS Wsap ON CAST(src.PHONE AS INT) = CAST(Wsap.PHNNUMBER AS INT);",
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
                        