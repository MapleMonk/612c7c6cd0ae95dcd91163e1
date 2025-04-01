{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.maplemonk.LOGIC_PU_CHALLAN_RETURN AS WITH DeduplicatedReturns AS ( SELECT DISTINCT TO_DATE(bill_date, \'DD/MM/YYYY\') AS bill_date, TO_DATE(vouch_date, \'DD/MM/YYYY\') AS vouch_date, account_code, account_name, branch_name, branch_code, bill_no, vouch_no, vouch_code, grn_number, action_code, quantity, grn_prefix, listitems, total_tax, net_amount, agent_name, doc_type FROM snitch_db.maplemonk.test_to_delete_get_pu_challan_return QUALIFY ROW_NUMBER() OVER (PARTITION BY bill_no, vouch_code, vouch_no, grn_number, branch_name ORDER BY bill_no) = 1 ), ExtractedItems AS ( SELECT dr.bill_date, dr.vouch_date, dr.account_code, dr.account_name, dr.branch_name, dr.branch_code, dr.bill_no, dr.vouch_no, dr.vouch_code, dr.grn_number, dr.action_code, dr.quantity, dr.grn_prefix, dr.total_tax, dr.net_amount, dr.agent_name, dr.doc_type, CASE WHEN UPPER( item.value:AddlItemCode::STRING) LIKE \'%T-SHIRT%\' OR UPPER( item.value:AddlItemCode::STRING) LIKE \'%CO-ORDS%\' THEN REGEXP_REPLACE( item.value:AddlItemCode::STRING, \'^([^\\-]+-[^\\-]+)-[^\\-]+-[^\\-]+-\', \'\\1-\') ELSE REGEXP_REPLACE( item.value:AddlItemCode::STRING, \'^([^\\-]+-[^\\-]+)-[^\\-]+-\', \'\\1-\') END AS SKU, item.value:Lot_Code::STRING AS lot_number, item.value:Quantity::NUMBER AS item_qty, FROM DeduplicatedReturns AS dr, LATERAL FLATTEN(INPUT => dr.listitems) AS item ) SELECT * FROM ExtractedItems;",
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
            