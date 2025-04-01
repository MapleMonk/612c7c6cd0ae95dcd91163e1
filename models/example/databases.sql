{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LOGIC_SALE_ORDER AS WITH DeduplicatedOrders AS ( SELECT DISTINCT order_no, listitems, agent_name, TO_DATE(order_date, \'DD/MM/YYYY\') AS ORDER_DATE, party_name, TO_DATE(valid_date, \'DD/MM/YYYY\') AS EXPIRY_DATE, vouch_code, action_code, order_number, order_prefix, branch_name, branch_short_name, party_order_no, party_user_code, net_order_amount, order_amount FROM snitch_db.maplemonk.logic_get_sale_order WHERE branch_short_name NOT IN (\'SAPL-WH2\', \'SAPL_EMIZA\', \'SAPL-WH1\', \'SAPL-SR\', \'(NIL)\', \'SAPL-B2B\', \'SURAT-VRMALL\') QUALIFY ROW_NUMBER() OVER (PARTITION BY order_no, vouch_code, branch_name ORDER BY order_no) = 1 ), ExtractedItems AS ( SELECT concat(do.order_no,do.branch_name) as uniquekey, do.order_no, CASE WHEN UPPER( item.value:ItemCode::STRING) LIKE \'%T-SHIRT%\' OR UPPER( item.value:ItemCode::STRING) LIKE \'%CO-ORDS%\' THEN REGEXP_REPLACE( item.value:ItemCode::STRING, \'^([^\\-]+-[^\\-]+)-[^\\-]+-[^\\-]+-\', \'\\1-\') ELSE REGEXP_REPLACE( item.value:ItemCode::STRING, \'^([^\\-]+-[^\\-]+)-[^\\-]+-\', \'\\1-\') END AS SKU, item.value:Quantity::NUMBER AS item_qty, item.value:Lot_Number::STRING AS lot_number, do.agent_name, do.ORDER_DATE, TRIM(RTRIM(REGEXP_SUBSTR(do.party_name, \'^[^-]*-[^-]*-[^-]*\', 1, 1), \'-\')) AS to_party, TRIM(RTRIM(REGEXP_SUBSTR(do.branch_name, \'^[^-]*-[^-]*-[^-]*\', 1, 1), \'-\')) AS from_party, do.EXPIRY_DATE, do.vouch_code, do.action_code, do.order_number, do.order_prefix, do.branch_short_name, do.party_order_no, do.party_user_code, do.net_order_amount, do.order_amount FROM DeduplicatedOrders AS do, LATERAL FLATTEN(INPUT => do.listitems) AS item ) SELECT CASE WHEN LEFT(Order_no,2) = \'SN\' THEN \'STORE-OMNI\' WHEN (to_party like \'%SNITCH - CO%\' or to_party like \'%SNITCH - FO%\') and (from_party like \'%SNITCH - CO%\' or from_party like \'%SNITCH - FO%\') THEN \'STORE-STORE\' WHEN to_party like \'%SNITCH - WH%\' and (from_party like \'%SNITCH - CO%\' or from_party like \'%SNITCH - FO%\') THEN \'STORE-WH\' ELSE NULL END AS ORIGIN, * FROM ExtractedItems",
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
            