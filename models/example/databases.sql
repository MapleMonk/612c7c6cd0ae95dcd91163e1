{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.IST_JOURNEY AS with sto as ( select origin, to_date(bill_date,\'DD/MM/YYYY\') as bill_date, to_party, from_party, bill_no as sto_bill_no, order_no, total_quantity, new_bill_no, NULLIF(CONCAT(LEFT(so_order_no, 3), SUBSTRING(so_order_no, 5)),\'\') AS SO_NO, UPPER(sku) AS STO_SKU, sum(item_quantity) as sku_qty, ean, item_mrp, trans_name, customer_name, branch_short_name, vouch_code, order_date, doc_number, bill_time from snitch_db.maplemonk.logic_final_sto where ORIGIN = \'STORE-STORE\' and from_party like \'SNITCH - CO%\' and bill_date >= \'2025-03-01\' AND SO_NO = \'SOO30\' and from_party like \'%KOR%\' group by 1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,20 ), sti as ( select bill_no, bill_date as sti_bill_date, to_party as sto_to_party, from_party as sto_from_party, vouch_no as sti_vouch, UPPER(sku) as sti_sku, SUM(item_quantity) as sti_qty from snitch_db.maplemonk.logic_final_sti where origin = \'STORE-STORE\' and from_party like \'SNITCH - CO%\' and sti_bill_date >= \'2025-03-01\' and from_party like \'%KORM%\' and bill_no = \'2425/19/STO-33\' group by 1,2,3,4,5,6 ), sto_sti as ( select * from sto FULL OUTER join sti on concat(sto.new_bill_no,sto.sto_sku) = concat(sti.bill_no,sti.sti_sku) ) select * from sto_sti ;",
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
            