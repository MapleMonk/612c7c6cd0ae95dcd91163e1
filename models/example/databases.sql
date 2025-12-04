{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table minimalist_db.maplemonk.B2b_accounting_table as select a.\"Company Name\" , a.\"Seller GST Num\" , b.\"B2B CUSTOMER NAME\" , a.\"ERP Customer ID\" , b.\"ORDER NO\" , b.\"INVOICE NO\" , a.\"Order Status\", \"ORDER DATE\", \"INVOICE DATE\", \"SKU ID\" SKU, \"Component SKU\", \"PRODUCT NAME\", \"COLOUR\", \"SIZE\", \"PRODUCT TAX CODE\", \"GRN UPC/EAN\", \"GRN Batch Code\", \"MANUFACTURING DATE\", \"EXPIRY DATE\", \"ACCOUNTING SKU\", \"ACCOUNTING UNIT\", \"QUANTITY\", \"Selling Price\", \"Item Price(Excluding Tax)\", \"Order Invoice Amount\", \"Taxable Value\", \"TAX\", \"IGST\", \"CGST\", \"SGST\", \"CESS\", \"UTGST\", \"TCS\", \"TDS\", \"Cost of Goods Purchased\", \"GRN Cost per unit\", \"Additional cost per unit\", \"Billing State\", \"Suborder No\" from minimalist_db.maplemonk.s3_tax_sales_report a left join minimalist_db.maplemonk.b2b_batch_wise_report b on a.\"EE Invoice No\" = b.\"INVOICE NO\" and replace(a.\"Parent SKU\",\'`\',\'\') = b.\"SKU ID\" ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from minimalist_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            