{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ritualistic_varee_fact_items as select sku, cast(tcs as float64) as tax_at_source, cast(tds as float64) as tax_deducted_at_source, cast(GSt_Rate as float64)/100 as GST_Rate, HSN_Code, cast(GST_on_COD as float64) as GST_on_COD, cast(GST_on_goods as float64) as GST_on_goods, parse_datetime(\'%d/%m/%y %H:%M\',order_date) Order_date, parse_date(\'%d/%m/%y\',Invoice_Date) Invoice_Date, Order_number as reference_code, Order_Status, upper(Customer_State) state, upper(customer_name) customer_name, Invoice_Number, Customer_GST_No_, cast(Total_Invoice_Value as float64) Total_Invoice_Value, cast(Taxable_value_of_goods as float64) Taxable_value_of_goods, cast(Taxable_value_of_COD_charges as float64) Taxable_value_of_COD_charges from maplemonk.ritualistic_sales_report_vaaree_gstr_report qualify row_number() over (partition by order_number,sku order by date(last_synced_date) desc) = 1 ;",
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
            