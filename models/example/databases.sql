{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_grn_cashflow_report as with base_data as ( select g.po_type, g.grn_id, g.po_detail_id, g.grn_status, g.vendor_name, g.po_created_date, g.grn_invoice_number as challan_number, g.po_ref_num, g.grn_created_date, g.grn_invoice_date, g.last_grn_date, g.grn_receive_quantity, DATE_ADD( cast(g.grn_created_date as date), INTERVAL GREATEST(45, IFNULL(safe_cast(regexp_extract(v.paymentTerm, r\'(\d+)\') as int64), 0)) DAY ) as payment_due_date, g.sku, safe_divide(g.total_grn_value,g.grn_receive_quantity) as grn_unit_value, g.total_grn_value as grn_total_value, qc.QC_Fail_Quantity, qc.QC_Pass_Quantity from maplemonk.saadaa_po_grn_mapping g LEFT JOIN `maplemonk.Easyecom_Saadaa_vendors` v on lower(trim(g.vendor_name)) = lower(trim(v.vendor_Name)) LEFT JOIN (SELECT PO, GRN, REPLACE(SKU,\'`\',\'\') SKU, SUM(QC_Fail_Quantity) QC_FAIL_QUANTITY, SUM(QC_Pass_Quantity) QC_PASS_QUANTITY FROM maplemonk.saadaa_easyecom_qc_report WHERE PO <> \'\' or GRN <> \'\' GROUP BY 1,2,3) qc on cast(qc.PO as string) = cast(g.po_id as string) and cast(qc.grn as string) = cast(g.grn_id as string) and lower(REPLACE(qc.sku,\'`\',\'\')) = lower(g.sku) ) select *, CASE WHEN EXTRACT(DAY FROM payment_due_date) <= 15 THEN DATE(EXTRACT(YEAR FROM payment_due_date), EXTRACT(MONTH FROM payment_due_date), 15) ELSE LAST_DAY(payment_due_date, MONTH) END as tentative_payment_date from base_data;",
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
            