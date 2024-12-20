{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.gs_cs_qa_wa as ( select \"Emp ID\", \"Associate Name\", \"Quality Analyst\" \"Team Lead\", \"Customer Name\", \"Chat ID\", TO_DATE(\"Call Date\", \'DD/MM/YYYY\') AS \"Call Date\", TO_DATE(\"Audit Date\", \'DD/MM/YYYY\') AS \"Audit Date\", \"CAMPAIGN\", \"ZONE\", \"CONCEPT\", \"TAG\", \"Associate used the standard greeting format (Opening/Closing)\", \"Assurance & Acknowledgement\", \"Ownership on chat/ Chat Opening (FRTAT) (within 1 Min)\", \"Grammer(Upper and lowercase/ Punctuation)\", \"Empathy/Sympathy\", \"Sentence Structure\", \"Additional Assistance offered/give an assurance to the User on customer support.\", \"Coherence (understanding the issue) being attentive on chat\", \"Chat Hold Procedure &Taking Permission before putting the chat on hold\", \"First chat resolution\", \"Rebuttal/Probing. First Chat / Email Resolution\", \"Email/Chat Avoidance (Chat TAT - 10Mins)\", \"Professional / Courtesy / Rude/ Disconnection\", \"Case study\", \"Process & Procedure Followed\", \"Accurate Resolution/Information is provided as per the process\", \"Observations of the call\", \"Market Observations/Feedbacks\", \"Overall Quality Score\" from snitch_db.maplemonk.gs_cs_qa_wa); create or replace table snitch_db.maplemonk.gs_cs_qa_ib as ( select \"Emp ID\", \"Associate Name\", \"Quality Analyst\", \"Team Lead\", \"Customer Name\", \"Call ID\", TO_DATE(\"Call Date\", \'DD/MM/YYYY\') AS \"Call Date\", TO_DATE(\"Audit Date\", \'DD/MM/YYYY\') AS \"Audit Date\", \"CAMPAIGN\", \"ZONE\", \"CONCEPT\", \"Call Duration\", \"Used Standard Opening Protocol\", \"Personalization ( Raport Building, Addressing by Name)\", \"Acknowledged Appropriately\", \"Active Listening without Interruption / Paraphrasing\", \"Used Empathetic Statements whenever required\", \"Tone & Intonation / Rate of Speech\", \"Took Ownership on the call\", \"Followed Hold Procedure Appropriately / Dead Air\", \"Offered Additional Assistance/Closed Call as per Protocol/ Give assurance to the User on customer support.\", \"First Call Resolution\", \"Probing / Tactful Finding / Rebuttal\", \"Complete Information Provided\", \"Professional / Courtesy\", \"Confirmed order ID\", \"Process & Procedure Followed\", \"Case study\", \"Observations of the call\", \"Market Observations/Feedbacks\", \"Overall Quality Score\" from snitch_db.maplemonk.gs_cs_qa_ib); create or replace table snitch_db.maplemonk.gs_cs_qa_dissat as ( select \"ADVISOR\", \"Conversation id\", \"TAG\", TO_DATE(\"DATE\", \'DD/MM/YYYY\') AS \"DATE\", \"CSAT Score\", \"ACPT\", \"ACPT type\", \"Customer feedback\" from snitch_db.maplemonk.gs_cs_qa_dissat)",
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
            