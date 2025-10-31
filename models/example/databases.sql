{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE store_issues_final AS SELECT TRY_CAST(Date_of_reporting AS DATE) AS Date_of_reporting, TRY_CAST(Last_updated_at AS TIMESTAMP) AS Last_updated_date, CASE WHEN LEFT(IMAGE, 6) = \'https:\' THEN IMAGE ELSE \'https:\' || IMAGE END AS IMAGE_WITH_HTTPS, id, created_at, store_name, reporter_name, reporter_email, Issue_Type, Issue_description, Image, Owner, Priority, TAT_Hours, Comments, Issue_assigned, issue_status, breach_status FROM store_issues;",
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
            