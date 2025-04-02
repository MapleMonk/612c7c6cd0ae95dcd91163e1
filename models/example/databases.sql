{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.SO_STO_ACCURACY AS with so as ( select ORIGIN, UNIQUEKEY, ORDER_NO, UPPER(SKU) AS SKU, ITEM_QTY, ORDER_DATE, TO_PARTY, FROM_PARTY, EXPIRY_DATE, VOUCH_CODE, ORDER_NUMBER from snitch_db.maplemonk.logic_sale_order where origin != \'STORE-OMNI\' ) , sto as ( select origin, to_date(bill_date,\'DD/MM/YYYY\') as sto_bill_date, to_party, from_party, bill_no, order_no, total_quantity, new_bill_no, NULLIF(CONCAT(LEFT(so_order_no, 3), SUBSTRING(so_order_no, 5)),\'\') AS SO_NO, UPPER(sku) AS SKU, sum(item_quantity) as sku_qty, ean, item_mrp, trans_name, customer_name, branch_short_name, vouch_code, order_date, doc_number, bill_time from snitch_db.maplemonk.logic_final_sto where ORIGIN in (\'STORE-WH\',\'STORE-STORE\') and bill_no not like \'%SSTO%\' and to_party not like \'%EMIZA%\' and so_order_no not in (\'\') and so_order_no is not null group by 1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,20 ), outward AS ( SELECT so.order_no, so.sku, item_qty as order_qty, so.order_date, so.to_party, so.from_party, expiry_date, so.vouch_code, so.origin, sto_bill_date, bill_no as sto_number, total_quantity, sto.sku as sto_sku, new_bill_no, sku_qty as sto_qty, bill_time from so FULL OUTER join sto on CONCAT(so.ORDER_NO,so.sku,so.from_PARTY) = CONCAT(sto.so_no,sto.sku,sto.from_party) ), so_sto as ( select * , IFNULL(ORDER_QTY,0) - IFNULL(STO_QTY,0) AS STORE_VARIANCE_OVERALL, CASE WHEN STORE_VARIANCE_OVERALL > 0 THEN STORE_VARIANCE_OVERALL ELSE 0 END AS STORE_SHORTAGE_OVERALL, CASE WHEN STORE_VARIANCE_OVERALL < 0 THEN ABS(STORE_VARIANCE_OVERALL) ELSE 0 END AS STORE_EXCESS_OVERALL, CASE WHEN bill_time is not null then TIMESTAMPDIFF(DAY, order_date,sto_bill_date) ELSE TIMESTAMPDIFF(DAY, order_date,current_date) END as AGEING from outward ) select * , coalesce(order_date,sto_bill_date)::DATE as DATE from so_STO order by order_date desc ;",
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
            