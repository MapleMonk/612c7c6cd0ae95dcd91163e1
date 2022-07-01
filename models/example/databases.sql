{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE dev.public.DEPARTMENT( ID INT PRIMARY KEY NOT NULL, DEPT CHAR(50) NOT NULL, EMP_ID INT NOT NULL );",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from dev.public.accounts
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        
