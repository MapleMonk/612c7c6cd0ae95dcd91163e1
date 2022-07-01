{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE dev.maplemonk.DEPARTMENT( ID INT PRIMARY KEY NOT NULL, DEPT CHAR(50) NOT NULL, EMP_ID INT NOT NULL );",
                                "transaction": true
                            }
                        ) }}
                      
                        
