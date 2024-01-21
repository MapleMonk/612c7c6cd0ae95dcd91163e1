{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "delete from xyxx_db.maplemonk._airbyte_raw_fieldassist_detailed_visit where _AIRBYTE_DATA:\"VisitId\" > 1192659486; delete from xyxx_db.maplemonk.fieldassist_detailed_visit_scd where visitid > 1192659486; delete from xyxx_db.maplemonk.fieldassist_detailed_visit where visitid > 1192659486; delete from xyxx_db.maplemonk.fieldassist_detailed_visit_sales where visitid > 1192659486;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        