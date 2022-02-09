{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "drop table if exists \"MM_TEST\".\"TEST\".\"newPersona\";create table \"MM_TEST\".\"TEST\".\"newPersona\" (storm varchar(25));",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VALUES('1')
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        
