{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table gladful_db.maplemonk.gladful_firstcry_fact_items as select try_cast(\"MRP\" as float) as MRP ,try_cast(\"QTY\" as float) as QTY ,\"SrNo.\" as SrNo ,try_cast(\"TOTAL\" as float) as TOTAL_SALES ,try_cast(\"CGST %\" as float) as CGST ,try_cast(\"SGST %\" as float) as SGST ,\"HSN Code\" as HSN_Code ,try_cast(\"Base Cost\" as float) as Base_Cost ,\"Order Ids\" as Order_Id ,try_to_date(\"Order Date\",\'DD/MM/YYYY\') as Order_Date ,\"Product ID\" as Product_ID ,try_Cast(\"CGST Amount\" as float) as CGST_Amount ,\"FC Ref. no.\" as FC_Ref_no ,try_cast(\"SGST Amount\" as float) as SGST_Amount ,try_to_Date(\"SR/RTO date\",\'DD/MM/YYYY\') as SRRTO_date ,try_cast(\"Gross Amount\" as float) as Gross_Amount ,try_to_date(\"Delivery date\",\'DD/MM/YYYY\') as Delivery_date ,try_to_Date(\"Shipping Date\",\'DD/MM/YYYY\') as Shipping_Date ,\"Debit note no.\" as Debit_note_no ,\"Payment advice no\" as Payment_advice_no ,\"Vendor Invoice no.\" as Vendor_Invoice_no ,\"_AIRBYTE_AB_ID\" as _AIRBYTE_AB_ID ,\"_AIRBYTE_EMITTED_AT\" as _AIRBYTE_EMITTED_AT ,\"_AIRBYTE_NORMALIZED_AT\" as _AIRBYTE_NORMALIZED_AT ,\"_AIRBYTE_GLADFUL_FIRSTCRY_SALES_HASHID\" as _AIRBYTE_GLADFUL_FIRSTCRY_SALES_HASHID from gladful_firstcry_sales; create or replace table gladful_db.maplemonk.gladful_jiomart_fact_items as select \"EAN\" as EAN ,try_cast(\"MRP\" as float) as MRP ,try_Cast(\"QTY\" as float) as QTY ,\"SKU\" as SKU ,\"Invoice Id\" as Invoice_Id ,try_cast(\"Item Total\" as float) as Item_Total ,try_to_timestamp(\"Accepted At\") as Accepted_At ,\"Product Title\" as Product_Title ,try_cast(\"Promotion Amt\" as float) as Promotion_Amt ,\"Tracking Code\" as Tracking_Code ,\"Fulfiller Name\" as Fulfiller_Name ,\"Shipment Number\" as Shipment_Number ,\"Shipment Status\" as Shipment_Status ,try_cast(\"Shipping Charge\" as float) as Shipping_Charge ,\"Fulfillment Type\" as Fulfillment_Type ,\"Payment Method Used\" as Payment_Method_Used ,try_to_timestamp(\"Shipment Created At\") as Shipment_Created_At ,\"Shipping Agent Code\" as Shipping_Agent_Code ,try_to_timestamp(\"Acceptance TAT Date & Time\") as Acceptance_TAT_Date_Time ,\"_AIRBYTE_AB_ID\" as _AIRBYTE_AB_ID ,\"_AIRBYTE_EMITTED_AT\" as _AIRBYTE_EMITTED_AT ,\"_AIRBYTE_NORMALIZED_AT\" as _AIRBYTE_NORMALIZED_AT ,\"_AIRBYTE_GLADFUL_JIOMART_SALES_HASHID\" as _AIRBYTE_GLADFUL_JIOMART_SALES_HASHID from gladful_jiomart_sales;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GLADFUL_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        