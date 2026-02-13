{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_zepto_po_fact_items; CREATE TABLE public.anveshan_zepto_po_fact_items AS SELECT \'ZEPTO\' as channel, \"ean\"::VARCHAR(50) as ean, \"hsn\"::VARCHAR(20) as hsn, \"sku\"::VARCHAR(100) as product_id, p.commonsku, p.category, \"brand\"::VARCHAR(50) as brand, \"po no.\"::VARCHAR(50) as po_number, \"status\"::VARCHAR(20) as po_status, \"sku code\"::VARCHAR(50) as sku_code, \"sku desc\"::VARCHAR(255) as sku_description, \"created by\"::VARCHAR(100) as created_by_email, \"vendor code\"::VARCHAR(50) as vendor_code, \"vendor name\"::VARCHAR(255) as vendor_name, \"del location\"::VARCHAR(255) as delivery_location, \"qty\"::INT, \"asn quantity\"::INT as asn_qty, \"grn quantity\"::INT as grn_qty, \"mrp\"::DECIMAL(18,2) as mrp, \"cess %\"::DECIMAL(5,2) as cess_pct, \"cgst %\"::DECIMAL(5,2) as cgst_pct, \"igst %\"::DECIMAL(5,2) as igst_pct, \"sgst %\"::DECIMAL(5,2) as sgst_pct, \"po amount\"::DECIMAL(18,2) as po_amount, \"landing cost\"::DECIMAL(18,2) as landing_cost, \"total amount\"::DECIMAL(18,2) as po_total_amount, \"unit base cost\"::DECIMAL(18,2) as unit_base_cost, TO_TIMESTAMP(\"po date\", \'DD Mon YYYY HH12:MI am\') as po_date_timestamp, TO_TIMESTAMP(\"po expiry date\", \'DD Mon YYYY HH12:MI am\') as po_expiry_timestamp, TO_DATE(\"polinelevel date\", \'YYYY-MM-DD\') as po_line_level_date FROM public.zepto_anveshan_po_line_level z LEFT JOIN ( SELECT * FROM ( SELECT master_sku as commonsku, zepto_sku as marketplace_sku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY zepto_sku ORDER BY LENGTH(COALESCE(zepto_sku, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON LOWER(REPLACE(REPLACE(z.\"sku\"::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.marketplace_sku) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            