{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anweshan_freshchat_conversations; create table public.anweshan_freshchat_conversations as SELECT _airbyte_data.id::VARCHAR AS id, _airbyte_data.conversation_id::VARCHAR AS conversation_id, _airbyte_data.user_id::VARCHAR AS user_id, _airbyte_data.updated_at::VARCHAR AS updated_at, _airbyte_data.messages::SUPER AS messages FROM ( SELECT _airbyte_data, _airbyte_emitted_at, _airbyte_ab_id, ROW_NUMBER() OVER (PARTITION BY _airbyte_data.id,_airbyte_data.conversation_id,_airbyte_data.updated_at ORDER BY _airbyte_emitted_at DESC) AS rn FROM public._airbyte_raw_freshchat_conversations ) WHERE rn = 1 ; DROP TABLE IF EXISTS public.anweshan_freshchat_groups; CREATE TABLE public.anweshan_freshchat_groups as select _airbyte_data.routing_type::VARCHAR AS routing_type, _airbyte_data.routing_logic::VARCHAR AS routing_logic, _airbyte_data.name::VARCHAR AS name, _airbyte_data.description::VARCHAR AS description, _airbyte_data.id::varchar AS id, _airbyte_data.chat_group_id::varchar AS chat_group_id from public._airbyte_raw_freshchat_groups ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            