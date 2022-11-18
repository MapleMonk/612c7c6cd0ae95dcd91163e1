{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "delete from xyxx_db.maplemonk._airbyte_raw_fieldassist_detailed_visit where visitid > 750000000; delete from xyxx_db.maplemonk.fieldassist_detailed_visit_scd where visitid > 750000000; delete from xyxx_db.maplemonk.fieldassist_detailed_visit where visitid > 750000000; delete from xyxx_db.maplemonk.fieldassist_detailed_visit_sales where visitid > 750000000;",
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
                        