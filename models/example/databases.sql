{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LOGIC_PURCHASE_ORDER AS WITH DeduplicatedOrders AS ( SELECT DISTINCT TO_DATE(order_date, \'DD/MM/YYYY\') AS ORDER_DATE, TO_DATE(nullif(delivery_date,\'\'), \'DD/MM/YYYY\') AS EDD, order_number, supplier_order_no, transfer_branch_code, branch_name as to_party, print_act_name as from_party, party_user_code, branch_short_name, vouch_code, order_prefix, action_code, net_order_amount, order_amount, listitems, total_tax, agent_name FROM snitch_db.maplemonk.logic_get_purchase_order QUALIFY ROW_NUMBER() OVER (PARTITION BY order_number, vouch_code, branch_name, print_act_name ORDER BY order_number) = 1 ), ExtractedItems AS ( SELECT CONCAT(ORDER_NUMBER,to_party,from_party) as uniquekey, do.ORDER_DATE, do.EDD, do.order_number, do.supplier_order_no, do.transfer_branch_code, do.to_party, do.from_party, do.party_user_code, do.branch_short_name, do.vouch_code, do.order_prefix, do.action_code, do.net_order_amount, do.order_amount, do.total_tax, do.agent_name, item.value:Logic_UserCode::STRING AS logic_user_code, item.value:Lot_Number::STRING AS lot_number, item.value:PO_MRP::NUMBER AS po_mrp, item.value:Rate::NUMBER AS po_cp, item.value:Pending_Qty::NUMBER AS pending_qty, item.value:Total_Qty::NUMBER AS total_qty, item.value:Txn_Code::NUMBER AS txn_code FROM DeduplicatedOrders AS do, LATERAL FLATTEN(INPUT => do.listitems) AS item ) SELECT CASE WHEN to_party like \'%B2B%\' then \'VENDOR-B2B\' WHEN to_party like \'%SNITCH - WH%\' and to_party not like \'%B2B%\' then \'VENDOR-WH\' WHEN to_party like \'%SNITCH - HO%\' then \'VENDOR-WQ\' ELSE NULL END AS ORIGIN , * FROM ExtractedItems",
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
            