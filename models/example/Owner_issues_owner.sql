{{ config(
            materialized='table',
                post_hook={
                    "sql": "",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.MAPLEMONK.Owner_issues_owner
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            