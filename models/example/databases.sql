{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.STO_JOURNEY_WAREHOUSE AS with sto as ( select origin, to_date(bill_date,\'DD/MM/YYYY\') as sto_bill_date, to_party, from_party, bill_no as sto_bill_no, order_no, total_quantity, new_bill_no, NULLIF(CONCAT(LEFT(so_order_no, 3), SUBSTRING(so_order_no, 5)),\'\') AS SO_NO, UPPER(sku) AS STO_SKU, sum(item_quantity) as sku_qty, ean, item_mrp, trans_name, customer_name, branch_short_name, vouch_code, order_date, doc_number, bill_time from snitch_db.maplemonk.logic_final_sto where ORIGIN = \'STORE-WH\' and from_party like \'SNITCH - CO%\' and sto_bill_date >= \'2025-03-01\' group by 1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,20 ), sti as ( select bill_no, vouch_date as sti_vouch_date, to_party as sto_to_party, from_party as sto_from_party, vouch_no as sti_vouch, UPPER(sku) as sti_sku, SUM(item_quantity) as sti_qty, doc_type, grn_prefix, account_code from snitch_db.maplemonk.logic_final_sti where origin = \'STORE-WH\' and from_party like \'SNITCH - CO%\' and sti_vouch_date >= \'2025-03-01\' group by 1,2,3,4,5,6,8,9,10 ), sto_sti as ( select * from sto LEFT JOIN sti on concat(sto.new_bill_no,sto.sto_sku) = concat(sti.bill_no,sti.sti_sku) ) select * from sto_sti ; select \"PO Code\" as PO_NUMBER, created, updated, UPPER(category) as CATEGORY, facility, TRIM(UPPER(\"Item SkuCode\")) as UC_SKU, \"Delivery Date\" as UC_DELIVERY_DATE, \"PO Created By\" as UC_CREATED_BY, \"Vendor Code\" as VENDOR_CODE, \"Item Type Name\", \"Order Quantity\"::INTEGER as PO_QTY, \"PO Approved Date\", \"Pending Quantity\"::INTEGER as UC_PENDING_QTY, UPPER(\"Rejection Reason\") as REASON_REJECT, \"Recieved Quantity\"::INTEGER AS RECEIVED_QTY, \"Rejected Quantity\"::INTEGER as REJECT_QTY, \"Purchase Order Status\" as PO_STATUS from snitch_db.maplemonk.uc_final_get_purchase_orders where (\"Vendor Code\") like \'%Mantri%\' and uc_sku = \'4MSS3391-01-M\' ;",
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
            