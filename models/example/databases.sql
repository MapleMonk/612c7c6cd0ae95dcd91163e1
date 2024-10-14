{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.firstresponse as SELECT A.*, B.max_created_date FROM ( SELECT *, DATE(\"Created date\") AS Created_date, ROW_NUMBER() OVER (PARTITION BY customer, DATE(\"Created date\") ORDER BY \"External ticket Id\") AS row_number FROM freshchat_bot_conversations WHERE \"STATUS\" = \'Closed\' AND \"Agent handover Type\" = \'Assigned to agent during conversation\' ) AS A LEFT JOIN ( SELECT Customer, MAX(\"Created date\") AS max_created_date,DATE(\"Created date\") AS Created_date FROM freshchat_bot_conversations WHERE \"STATUS\" = \'Closed\' AND flow <> \'Resolve - Campaigns flow\' GROUP BY Customer,DATE(\"Created date\") ) AS B ON (B.Customer = A.Customer AND B.max_created_date > A.\"Created date\" and B.Created_date= A.Created_date); CREATE OR REPLACE TABLE snitch_db.maplemonk.Order_Over_Issues_v3 AS SELECT orders.ORDER_DATE, orders.daily_order_count, COALESCE(ticket_counts.daily_ticket_count, 0) AS daily_ticket_count, COALESCE((COALESCE(ticket_counts.daily_ticket_count, 0) * 100.0) / NULLIF(orders.daily_order_count, 0), 0) AS ticket_percentage FROM ( select COUNT(DISTINCT order_id) AS daily_order_count,date(ORDER_TIMESTAMP) as ORDER_DATE from snitch_db.maplemonk.fact_items_snitch where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') AND ORDER_TIMESTAMP >= \'2024-07-01\' GROUP BY date(ORDER_TIMESTAMP) ) AS orders LEFT JOIN ( SELECT CAST( \"Created date\" as date) AS Converted_Created_At, count(DISTINCT \"External ticket Id\") AS daily_ticket_count FROM maplemonk.freshchat_bot_conversations WHERE CAST( \"Created date\" as date) >= \'2024-07-01\' and status =\'Closed\' and flow <> \'Resolve - Campaigns flow\' GROUP BY CAST( \"Created date\" as date) ) AS ticket_counts ON orders.ORDER_DATE = ticket_counts.Converted_Created_At ORDER BY orders.ORDER_DATE DESC;",
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
            