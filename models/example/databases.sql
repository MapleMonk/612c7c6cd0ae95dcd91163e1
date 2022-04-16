{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "DROP TABLE TEST.RANDOM.TESTCHECKIT1; CREATE TABLE TEST.RANDOM.TESTCHECKIT1 (name varchar(30));",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        