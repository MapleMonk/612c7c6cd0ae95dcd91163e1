{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.PURCHASE_RETURN AS WITH ranked_purchase_return AS ( SELECT bill_no, bill_date, vouch_no, grn_number, vouch_code, vouch_date, branch_code, CASE WHEN ACCOUNT_NAME like \'%SNITCH%\' THEN TRIM(RTRIM(REGEXP_SUBSTR(account_name, \'^[^-]*-[^-]*-[^-]*\', 1, 1), \'-\')) ELSE account_name END AS to_party, CASE WHEN BRANCH_NAME like \'%SNITCH%\' THEN TRIM(RTRIM(REGEXP_SUBSTR(branch_name, \'^[^-]*-[^-]*-[^-]*\', 1, 1), \'-\')) ELSE branch_name END AS from_party, listitems, net_amount, total_tax, agent_name, doc_type, CONCAT(branch_name, bill_no, vouch_date) AS unique_code, ROW_NUMBER() OVER (PARTITION BY branch_name, bill_no, vouch_date ORDER BY _AIRBYTE_EMITTED_AT DESC) AS rn FROM snitch_db.maplemonk.get_purchase_return ) SELECT f.unique_code, f.from_party, f.to_party, f.bill_no, f.vouch_date, f.bill_date, f.grn_number, f.vouch_code, f.branch_code, l.value:\"AddlItemCode\"::STRING AS SKU, l.value:\"HSN\"::STRING AS HSN, SUM(l.value:\"Quantity\"::NUMBER) AS item_qty, f.net_amount, f.total_tax, f.agent_name, f.doc_type FROM ranked_purchase_return f, LATERAL FLATTEN(input => f.listitems) l WHERE rn = 1 GROUP BY f.unique_code, f.from_party, f.bill_no, f.to_party, f.vouch_date, f.bill_date, f.grn_number, f.vouch_code, f.branch_code, SKU, HSN, f.net_amount, f.total_tax, f.agent_name, f.doc_type;",
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
            