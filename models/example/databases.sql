{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE dev.maplemonk.DEPARTMENT( ID INT PRIMARY KEY NOT NULL, DEPT CHAR(50) NOT NULL, EMP_ID INT NOT NULL );",
                                "transaction": true
                            }
                        ) }}
                      
                          with sample_data as (

                            select * from STV_TBL_PERM
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
