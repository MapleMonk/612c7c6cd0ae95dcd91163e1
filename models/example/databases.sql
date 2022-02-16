{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "DROP table IF EXISTS "MM_TEST"."TEST"."NEWPERSONA"; create table MM_TEST.TEST.NEWPERSONA (name varchar(25));",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MM_TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        