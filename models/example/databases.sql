{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "--checking comments are working or not INSERT INTO select_db.maplemonk.test_comments (id, name, age) VALUES (7, \'Emma\', 29), (8, \'James\', 33), --test case 1 --test case 2 (9, \'Olivia\', 27), (10, \'William\', 31), (11, \'Isabella\', 26);",
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
                        