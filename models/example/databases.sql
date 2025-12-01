{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE store_issues_updated AS SELECT s.*, m.\"STORE\" AS Store_Owners, m.\"Area VMs\" AS Area_VMS, m.\"Area Manager\" AS Area_Manager, CASE WHEN s.Issue_type IN (\'VM props\', \'Lit Visuals\') THEN m.\"Area VMs\" ELSE o.\"OWNER\" END AS Issues_Owner FROM store_issues_final AS s LEFT JOIN final_store_mapping AS m ON TRIM(SPLIT_PART(s.STORE_NAME, \'-\', 3)) = TRIM(m.\"Store Name\") LEFT JOIN owner_issues_owner AS o ON s.Issue_type = o.Issues;",
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
            