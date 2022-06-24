{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE dev.maplemonk.personalD( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255) );",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from dev.maplemonk.bigecom
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        
