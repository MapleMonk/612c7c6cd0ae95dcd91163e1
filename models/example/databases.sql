{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ras-wh.maplemonk.Nykaa_Dispatch_Tracker as select cast(gap as int64) as GAP, cast(mrp as float64) as MRP, upper(Brand) Brand, \'NYKAA\' as Marketplace, PO_No_ as PO_Number, cast(PO_Qty as int64) as PO_Quantity, Unique, AWB_No_ as AWB, cast(Box_No_ as int64) as Box_Number, case when grn_date <> \'-\' then cast(PARSE_DATE(\'%d/%m/%Y\', TRIM(CAST(GRN_Date AS STRING))) as date)else null end AS GRN_Date, cast(GRN_Qty as int64) as GRN_Quantity, cast(PARSE_DATE(\'%e-%b-%Y\', CONCAT(TRIM(PO_Date), \'-2025\')) as date) AS PO_date, Status as PO_Status, Tagging, ERP_Code as SKU, EXTRACT(MONTH FROM PARSE_DATE(\'%e-%b-%Y\', CONCAT(TRIM(PO_Date), \'-2025\'))) AS PO_month, NykaaCode, Invoice_No_ as Invoice_Number, cast(Open_Po_Qty as int64) as Open_PO_Quantity, FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%d-%b-%y\', Invoice_Date)) AS Invoice_date, Product_name, cast(Supplied_Qty as int64) as Supplied_Quantity, cast(InTransit_Qty as int64) as InTransit_Qty, cast(replace(Invoice_Value,\',\',\'\') as float64) as Total_Invoice_Value, EXTRACT(MONTH FROM PARSE_DATE(\'%e-%b-%Y\', CONCAT(TRIM(Delivered_Date), \'-2025\'))) AS Delivered_Date, cast(replace(Supplied_Value,\',\',\'\') as int64) as Supplied_Value, EXTRACT(MONTH FROM PARSE_DATE(\'%e-%b-%Y\', CONCAT(TRIM(Amount_Due_date), \'-2025\'))) AS Amount_Due_Date, Dispatch_Month, PO_No_NykaaCode, cast(PO_Status_Count as int64) PO_Status_Count, TaggingPO_Month, cast(Unfulfilled_Qty as int64) Unfulfilled_Qty, Logistic_Partner, case when Appointment_Dates not like \'%Appointment Date Not%\' then EXTRACT(MONTH FROM PARSE_DATE(\'%e-%b-%Y\', CONCAT(TRIM(Appointment_Dates), \'-2025\'))) else null end AS Appointment_Dates, case when Fullfilment_Ratio not like \'%#%\' then cast(cast(replace(Fullfilment_Ratio,\'%\',\'\') as int64)/100 as float64) else null end as Fulfilment_ratio, Warehouse_Location, Unique_for_PO_Count, TaggingDispatch_Month, cast(Time_Lag_in_Appointments as int64) Time_Lag_in_Appointments, Unique_for_PO_Monthwise_Dispatch from ras-wh.`MAPLEMONK.DISPATCH_TRACKER_NYKAA` ;",
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
            