{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace Table futwork_data_cleaned AS SELECT CITY, MOBILE, STATUS, CHANNEL, LANGUAGE, USER_NAME, TO_TIME(TRY_TO_TIMESTAMP(\"1-Start Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"1-Start Time\", TO_TIME(TRY_TO_TIMESTAMP(\"1-End Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"1-End Time\", TRY_TO_NUMBER(\"1-Minutes Spoken\") AS \"1-Minutes Spoken\", \"1-Call Direction\", \"1-Call Outcome\", \"1-DisconnectedBy\", \"1-Feedback\", \"1-Recording\", TO_TIME(TRY_TO_TIMESTAMP(\"2-Start Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"2-Start Time\", TO_TIME(TRY_TO_TIMESTAMP(\"2-End Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"2-End Time\", TRY_TO_NUMBER(\"2-Minutes Spoken\") AS \"2-Minutes Spoken\", \"2-Call Direction\", \"2-Call Outcome\", \"2-DisconnectedBy\", \"2-Feedback\", \"2-Recording\", TO_TIME(TRY_TO_TIMESTAMP(\"3-Start Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"3-Start Time\", TO_TIME(TRY_TO_TIMESTAMP(\"3-End Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"3-End Time\", TRY_TO_NUMBER(\"3-Minutes Spoken\") AS \"3-Minutes Spoken\", \"3-Call Direction\", \"3-Call Outcome\", \"3-DisconnectedBy\", \"3-Feedback\", \"3-Recording\", TO_TIME(TRY_TO_TIMESTAMP(\"4-Start Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"4-Start Time\", TO_TIME(TRY_TO_TIMESTAMP(\"4-End Time\", \'YYYY-MM-DD HH24:MI:SS\')) AS \"4-End Time\", TRY_TO_NUMBER(\"4-Minutes Spoken\") AS \"4-Minutes Spoken\", \"4-Call Direction\", \"4-Call Outcome\", \"4-DisconnectedBy\", \"4-Feedback\", \"4-Recording\", TRY_TO_NUMBER(\"Total Mins\") AS \"Total Mins\", TO_DATE(TRY_TO_TIMESTAMP(\"Upload Date\", \'MMMM DD, YYYY HH12:MI AM\')) AS \"Upload Date\", TRY_TO_NUMBER(NET_SPEND_L12) AS NET_SPEND_L12, ORDER_CHANNEL, TRY_TO_NUMBER(\"Attempts count\") AS \"Attempts count\", TO_DATE(TRY_TO_TIMESTAMP(\"Conclusion Date\", \'MMMM DD, YYYY HH12:MI AM\')) AS \"Conclusion Date\", TRY_TO_NUMBER(\"Remaining Attempts\") AS \"Remaining Attempts\", TO_DATE(TRY_TO_TIMESTAMP(LAST_TRANSACTION_DATE, \'YYYY-MM-DD HH24:MI:SS\')) AS LAST_TRANSACTION_DATE, TRY_TO_NUMBER(\"Connected Attempts count\") AS \"Connected Attempts count\", \"Final Disposition\", \"Not Interested Reason\", \"Not Interested Reason - Other\", _AB_SOURCE_FILE_URL, _AB_ADDITIONAL_PROPERTIES, _AB_SOURCE_FILE_LAST_MODIFIED, \"Language Barrier language spoken in\", \"Summarize call discussion for follow up\", \"Summarize the call for future reference\", TO_DATE(TRY_TO_TIMESTAMP(\"Note down the date and time for follow up\", \'MMMM DD, YYYY HH12:MI AM\')) AS \"Note down the date and time for follow up\", TO_DATE(TRY_TO_TIMESTAMP(\"Note down the date and time to call back later\", \'MMMM DD, YYYY HH12:MI AM\')) AS \"Note down the date and time to call back later\" FROM futwork_charts_futwork_data;",
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
            