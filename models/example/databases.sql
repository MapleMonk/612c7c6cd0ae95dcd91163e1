{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.firstresponse as SELECT A.*, B.max_created_date FROM ( SELECT *, DATE(\"Created date\") AS Created_date, ROW_NUMBER() OVER (PARTITION BY customer, DATE(\"Created date\") ORDER BY \"External ticket Id\") AS row_number FROM freshchat_bot_conversations WHERE \"STATUS\" = \'Closed\' AND \"Agent handover Type\" = \'Assigned to agent during conversation\' ) AS A LEFT JOIN ( SELECT Customer, MAX(\"Created date\") AS max_created_date,DATE(\"Created date\") AS Created_date FROM freshchat_bot_conversations WHERE \"STATUS\" = \'Closed\' AND flow <> \'Resolve - Campaigns flow\' GROUP BY Customer,DATE(\"Created date\") ) AS B ON (B.Customer = A.Customer AND B.max_created_date > A.\"Created date\" and B.Created_date= A.Created_date);",
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
            