{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Bot_csat as SELECT conversation_id, message_data.value:created_time::timestamp AS created_time, message_part.value:text:content::string AS message_content FROM freshchat_msg_final_v3_conversations_by_id, LATERAL FLATTEN(input => messages) AS message_data, LATERAL FLATTEN(input => message_data.value:message_parts) AS message_part WHERE message_data.value:actor_type::string = \'user\' AND message_part.value:text:content::string IN (\'Very Happy\', \'Happy\', \'Not Satisfied\', \'Sad\', \'Extremely Sad\') CREATE OR REPLACE TABLE snitch_db.maplemonk.contactratereasons AS WITH messages_exploded AS ( SELECT m.value:conversation_id::string AS conversation_id, m.value:created_time::timestamp AS created_time, m.value:actor_type::string AS actor_type, m.value:message_parts AS message_parts FROM freshchat_msg_final_v3_conversations_by_id fc, LATERAL FLATTEN(input => fc.MESSAGES) m ) SELECT DISTINCT t.conversation_id, t.created_time, t.content_msg FROM ( SELECT me.*, ROW_NUMBER() OVER (PARTITION BY me.conversation_id ORDER BY me.created_time ASC) AS rn, me.message_parts[0]:text:content::string AS content_msg FROM messages_exploded me WHERE me.actor_type = \'user\' ) t WHERE t.rn = 2",
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
            