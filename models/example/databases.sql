{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT \"ITEM CODE\", \"BRANCH_CODE_PRIORITY\", \"BRANCH NAME\", SUM(qty) AS qty FROM SNITCH_DB.MAPLEMONK.JIT_OFFLINE_GOODS GROUP BY 1, 2, 3",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            