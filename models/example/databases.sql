{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Zouk_Unresolved_Ticket_Report AS WITH agents_parsed AS ( SELECT id AS agent_id, JSON_VALUE(contact, \'$.name\') AS agent_name FROM `MapleMonk.Zouk_agents` ), Order_Level_Data AS ( SELECT order_timestamp, ORDER_NAME FROM `MapleMonk.zouk_SHOPIFY_FACT_ITEMS` where lower(marketplace) not like \'%pos%\' ), tickets_with_status AS ( SELECT id AS ticket_id, responder_id, type, JSON_VALUE(stats, \'$.resolved_at\') AS resolved_at, JSON_VALUE(stats, \'$.closed_at\') AS closed_at, PARSE_DATETIME(\'%Y-%m-%dT%H:%M:%SZ\', created_at) AS created_at_dt, PARSE_DATETIME(\'%Y-%m-%dT%H:%M:%SZ\', updated_at) AS updated_at_dt FROM `MapleMonk.Zouk_tickets` ) SELECT COALESCE(t.created_at_dt, o.order_timestamp) AS created_at, o.order_name as Order_ID, a.agent_id, a.agent_name, t.type, t.ticket_id, t.resolved_at, t.closed_at, t.updated_at_dt AS updated_at FROM Order_Level_Data o FULL OUTER JOIN Tickets_with_status t ON Date(o.order_timestamp) = Date(t.created_at_dt) LEFT JOIN agents_parsed a ON t.responder_id = a.agent_id ORDER BY created_at ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            