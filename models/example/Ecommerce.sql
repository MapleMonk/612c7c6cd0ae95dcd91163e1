{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "DROP table  IF EXISTS \"MM_TEST\".\"TEST\".\"NEWPERSONA\";
create table \"MM_TEST\".\"TEST\".\"NEWPERSONA\" (name varchar(25));",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (
                            select * from (VALUES (1))
                        ),
                        final as (
                            select * from sample_data
                        )
                        select * from final
