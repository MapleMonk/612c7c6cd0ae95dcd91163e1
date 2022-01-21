{{ config(
            materialized='table',
                post_hook={
                    "sql": "create table demo_db.batchtest.newPersona(storm varchar(25));",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DEMO_DB.batchtest.sqltest_BigEcom
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            