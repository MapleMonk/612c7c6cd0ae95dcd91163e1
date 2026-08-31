{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.rar_tally_gst_fact_items as (select cast(qty as int64) as quantity, cast(cess as float64) as cess, cast(cgst as float64) as cgst, cast(IGST as float64) as igst, cast(sgst as float64) as sgst, cast(parse_date(\'%d-%m-%Y\',Date) as date) as Date, cast(parse_date(\'%d-%m-%Y\',Original_Invoice_Date) as date) as Invoice_date, cast(sales as float64) as sales, cast(total as float64) as total, cast(UTGST as float64) as utgst, entity, godown, awb_num, currency, hsn_code, replace(cess_rate,\'Cess Rate\',\'\') as cess_rate, replace(IGST_Rate,\'IGST\',\'\') as igst_rate, replace(CGST_Rate,\'CGST\',\'\') as cgst_rate, replace(SGST_Rate,\'SGST\',\'\') as sgst_rate, replace(UTGST_Rate,\'UTGST\',\'\') as utgst_rate, cast(TCS_Amount as float64) as TCS_Amount, cast(unit_price as float64) as unit_price, upper(Product_Name) as product_name, cast(store_credit as float64) as store_credit, cast(Channel_entry as float64) as channel_entity, channel_ledger as channel, Invoice_number, payment_method, cast(Discount_Amount as float64) as discount, cast(conversion_rate as float64) as conversion_rate, original_sale_no, upper(Product_SKU_Code) as sku, sale_order_number, Billing_Party_Code, channel_party_gstin, shipping_address_city as city, Shipping_Address_State as state, Shipping_Address_Pincode as pincode, cast(Adjustment_In_Selling_Price as float64) as Adjustment_In_Selling_Price from maplemonk.unicommerce_get_tally_gst_report qualify row_number() over (partition by original_sale_no,product_sku_code,date order by date(Date), cast(parse_date(\'%d-%m-%Y\',Original_Invoice_Date) as date))=1 ) union all select quantity, 0 as cess, 0 as cgst, 0 as igst, 0 as sgst, date(created_date) as order_date, date(created_date) as order_date, (selling_price - tax_recovery) as sales, selling_price, 0 as ugst, null as entity, cast(warehouse_id as string) as warehouse, order_tracking_number, \'INR\' as currency, null as hsn_code, \'0.0%\' as cess_rate, \'0.0%\' as igst_rate, \'0.0%\' as cgst_rate, \'0.0%\' as sgst_rate, \'0.0%\' as utgst_rate, 0 as tcs_amount, (selling_price - tax_recovery) as unit_price, product_name, 0 as store_credit, (selling_price - tax_recovery) as channel_entity, channel, null as invoice_number, null as payment_method, discount, 0 as conversion_rate, order_id as original_sale_no, commonsku, order_id, null as Billing_Party_Code, null as channel_party_gstin, city, state, null as pincode, 0 as adjustment_in_sale from maplemonk.RAR_Myntra_Orders_Fact_Items ;",
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
            