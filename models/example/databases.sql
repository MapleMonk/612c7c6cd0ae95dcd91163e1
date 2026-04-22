{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LOGIC_PURCHASE_ORDER AS WITH base AS ( SELECT TO_DATE(order_date, \'DD/MM/YYYY\') AS order_date, TO_DATE(NULLIF(delivery_date, \'\'), \'DD/MM/YYYY\') AS edd, order_number, supplier_order_no, transfer_branch_code, branch_name AS to_party, print_act_name AS from_party, party_user_code, branch_short_name, vouch_code, order_prefix, action_code, net_order_amount, order_amount, listitems, total_tax, agent_name FROM snitch_db.maplemonk.logic_get_purchase_order ), ExtractedItems AS ( SELECT CONCAT(order_number, to_party, from_party) AS uniquekey, b.order_date, b.edd, b.order_number, b.supplier_order_no, b.transfer_branch_code, b.to_party, b.from_party, b.party_user_code, b.branch_short_name, b.vouch_code, b.order_prefix, b.action_code, b.net_order_amount, b.order_amount, b.total_tax, b.agent_name, item.value[\'Logic_UserCode\']::STRING AS logic_user_code, item.value[\'EAN_Code\']::STRING AS ean_code, item.value[\'Lot_Number\']::STRING AS lot_number, item.value[\'PO_MRP\']::NUMBER AS po_mrp, item.value[\'Rate\']::NUMBER AS po_cp, item.value[\'Pending_Qty\']::NUMBER AS pending_qty, item.value[\'Total_Qty\']::NUMBER AS total_qty, item.value[\'Txn_Code\']::NUMBER AS txn_code FROM base b, LATERAL FLATTEN(INPUT => PARSE_JSON(b.listitems)) AS item ), DeduplicatedItems AS ( SELECT * FROM ExtractedItems QUALIFY ROW_NUMBER() OVER ( PARTITION BY order_number, vouch_code, to_party, from_party, COALESCE(ean_code, logic_user_code), lot_number, txn_code ORDER BY order_number ) = 1 ), ean AS ( SELECT * FROM snitch_db.maplemonk.ean_to_sku_mapping ), final1 AS ( SELECT ex.*, COALESCE(en.sku, ex.logic_user_code) AS sku, UPPER( TRIM( REGEXP_REPLACE(COALESCE(en.sku, ex.logic_user_code)::STRING, \'-[^-]+$\', \'\') ) ) AS sku_group FROM DeduplicatedItems ex LEFT JOIN ean en ON ex.logic_user_code = en.ean ) SELECT CASE WHEN to_party LIKE \'%B2B%\' THEN \'VENDOR-B2B\' WHEN to_party LIKE \'%SNITCH - WH%\' AND to_party NOT LIKE \'%B2B%\' THEN \'VENDOR-WH\' WHEN to_party LIKE \'%SNITCH - HO%\' THEN \'VENDOR-WQ\' ELSE NULL END AS origin, * FROM final1;",
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
            