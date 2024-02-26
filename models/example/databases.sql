{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or Replace Table HOX_DB.MapleMonk.HOX_BLANKO_Amazon_Tax_Fact_Items as select case when lower(\"Transaction Type\") = \'refund\' then \'Amazon Return\' when lower(\"Transaction Type\") = \'freereplacement\' then \'Amazon Replacement\' else \'Amazon Sales\' end as report_type, case when lower(\'Amazon\' || \' \' || \"Ship From City\") in (\'amazon bengaluru\', \'amazon bangalore\') then \'Amazon Bangalore\' else lower(\'Amazon\' || \' \' || \"Ship From City\") end as Company_Name, \"Seller Gstin\" as seller_gst, \"Order Id\" as Reference_Code, \'Amazon B2B\' as order_type, lower(\"Transaction Type\") as Order_status, case when lower(\"Transaction Type\") in (\'cancel\', \'einvoicecancel\') then \'sold(cancelled)\' else \'sold\' end as Invoice_status, case when lower(\"Transaction Type\") = \'refund\' then ifnull(try_cast(\"Credit Note Date\" as timestamp),try_to_timestamp(\"Credit Note Date\" ,\'dd/mm/yy hh:mi\')) else ifnull(try_cast(\"Invoice Date\" as timestamp),try_to_timestamp(\"Invoice Date\" ,\'dd/mm/yy hh:mi\')) end as Report_date, try_cast(Quantity as float) as Quantity, SKU, try_cast(\"Invoice Amount\" as float) as Invoice_amount, abs(try_cast(\"Invoice Amount\" as float)) as absolute_invoice_amount, try_cast(\"Tax Exclusive Gross\" as float) as Tax_exclusive_gross, try_cast(\"Total Tax Amount\" as float) as Total_tax_amount from hox_db.maplemonk.hox_blanko_amazon_b2b_tax_report union Select case when lower(\"Transaction Type\") = \'refund\' then \'Amazon Return\' when lower(\"Transaction Type\") = \'freereplacement\' then \'Amazon Replacement\' else \'Amazon Sales\' end as report_type, case when lower(\'Amazon\' || \' \' || \"Ship From City\") in (\'amazon bengaluru\', \'amazon bangalore\') then \'Amazon Bangalore\' else lower(\'Amazon\' || \' \' || \"Ship From City\") end as Company_Name, \"Seller Gstin\" as seller_gst, \"Order Id\" as Reference_Code, \'Amazon B2C\' as order_type, lower(\"Transaction Type\") as Order_status, case when lower(\"Transaction Type\") = \'cancel\' then \'sold(cancelled)\' else \'sold\' end as Invoice_status, case when lower(\"Transaction Type\") = \'refund\' then ifnull(try_cast(\"Credit Note Date\" as timestamp),try_to_timestamp(\"Credit Note Date\" ,\'dd/mm/yy hh:mi\')) else ifnull(try_cast(\"Invoice Date\" as timestamp),try_to_timestamp(\"Invoice Date\" ,\'dd/mm/yy hh:mi\')) end as Report_date, try_cast(Quantity as float) as Quantity, SKU, try_cast(\"Invoice Amount\" as float) as Invoice_amount, abs(try_cast(\"Invoice Amount\" as float)) as absolute_invoice_amount, try_cast(\"Tax Exclusive Gross\" as float) as Tax_exclusive_gross, try_cast(\"Total Tax Amount\" as float) as Total_tax_amount from hox_db.maplemonk.hox_blanko_amazon_tax_report_b2c",
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
                        