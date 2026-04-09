{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.Sirona_MP_PO_Tracker_FactItems as select PO_Number ,Appointment_Id ,Appointment_Date ,Appointment_Time ,Status ,Remarks ,Invoice_No ,Delivery_Date ,Delivery_Remarks ,b.* from `Maplemonk.Sirona_DB_GS_MP_DATA` a LEFT JOIN( select Warehouse_name ,DATE(order_date) ORDER_Date ,FORMAT_DATE(\'%b\', DATE(order_date)) AS month_name ,awb ,city ,state ,reference_code ,customer_name ,sum(suborder_quantity) Quantity ,SUM(selling_price) AS selling_price FROM maplemonk.sirona_wh_EasyEcom_FACT_ITEMS group by 1,2,3,4,5,6,7,8 ) b on a.PO_Number = b.reference_code",
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
            