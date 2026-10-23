{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.RTO_fact_items as select distinct Reason_For_Last_Failed_Delivery, safe_cast(COD_Value as float64) COD_Value, safe_cast(Invoice_Value as float64) Invoice_Value, Drop_State, Drop_Pincode as Pincode, Order_Date, Invoice_Date, safe_cast(Items_Quantity as float64) as Items_Quantity, zone, Product_SKU_Code, safe_cast(Shipping_Cost as float64) as Shipping_Cost, safe_cast(Shipment_Breadth as float64) as Shipment_Breadth, safe_cast(Shipment_Weight as float64) as Shipment_Weight, safe_cast(Shipment_Length as float64) as Shipment_Length, safe_cast(Shipment_Height as float64) as Shipment_Height, Out_For_Delivery_1st_Attempt as Out_For_Delivery_1st_Attempt, Order_ID, AWB, payment_mode, courier_partner, Clickpost_Unified_Status from maplemonk.GoodTribe_Clickpost_track_order_dashboard_report ; select * from maplemonk.GoodTribe_Clickpost_track_order_dashboard_report limit 1",
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
            