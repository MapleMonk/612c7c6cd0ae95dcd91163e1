{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_Easyecom_Tax_Fact_Items as select \'Easyecom sale\' as report_type, case when trim(UPPER(\"Company Name\")) in (\'BLANKO GGN\', \'MOJOJOJO CREATORS PVT LTD GGN\') then \'BLANKO GGN\' when trim(upper(\"Company Name\")) in (\'BLANKO BLR\', \'MOJOJOJO CREATORS PVT LTD BLR\') then \'BLANKO BLR\' else \"Company Name\" end as Company_Name, \"Seller GST Num\" as seller_gst, case when \"Reference Code\" = \'HOX_BLANKO_0070_PR\' then \'Shopify\' else \"MP Name\" end as marketplace, \"Reference Code\" as Reference_Code, case when \"Reference Code\" = \'HOX_BLANKO_0070_PR\' then \'B2C\' else \"Order Type\" end as order_type, \"Order Status\" as Order_status, \"Invoice Status\" as Invoice_status, try_cast(\"Invoice Date\" as timestamp) as Report_date, try_cast(\"Item Quantity\" as float) as Quantity, replace(\"Parent SKU\", \'`\', \'\') as SKU, \"Component SKU Name\" as component_sku_name, try_cast(\"Order Invoice Amount\" as float) as Invoice_amount, try_cast(\"Selling Price\" as float) as Selling_price, abs(try_cast(\"Order Invoice Amount\" as float)) as absolute_invoice_amount, try_cast(\"Taxable Value\" as float) as Tax_exclusive_gross, try_cast(\"TAX\" as float) as Total_tax_amount from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_sales WHERE \"Reference Code\" not like \'%_DEL_%\' and \"Order Type\" <> \'STN\' union all select \'Easyecom return\' as report_type, case when trim(UPPER(\"Company Name\")) in (\'BLANKO GGN\', \'MOJOJOJO CREATORS PVT LTD GGN\') then \'BLANKO GGN\' when trim(upper(\"Company Name\")) in (\'BLANKO BLR\', \'MOJOJOJO CREATORS PVT LTD BLR\') then \'BLANKO BLR\' else \"Company Name\" end as Company_Name, \"Seller GST Num\" as seller_gst, \"MP Name\" as marketplace, \"Reference Code\" as Reference_Code, \"Order Type\" as order_type, \"Order Status\" as Order_status, \"Invoice Status\" as Invoice_status, try_cast(\"Return Date\" as timestamp) as Report_date, try_cast(\"Item Quantity\" as float) as Quantity, replace(\"Parent SKU\", \'`\', \'\') as SKU, \"Component SKU Name\" as component_sku_name, (try_cast(\"Order Invoice Amount\" as float) * -1.00) as Invoice_amount, try_cast(\"Selling Price\" as float) as Selling_price, abs(try_cast(\"Order Invoice Amount\" as float)) as absolute_invoice_amount, try_cast(\"Taxable Value\" as float) as Tax_exclusive_gross, try_cast(\"TAX\" as float) as Total_tax_amount from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_returns WHERE \"Reference Code\" not like \'%_DEL_%\' and \"Reference Code\" <> \'Amazon_FBA_Blanko_13\'",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        