{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.Sirona_MP_PO_Tracker_FactItems as select PO_Number ,Appointment_Id ,Appointment_Date ,Appointment_Time ,Status ,Remarks ,Invoice_No ,Delivery_Date ,Delivery_Remarks ,b.* ,channel ,d.Sub_Channel from `Maplemonk.Sirona_DB_GS_MP_DATA` a LEFT JOIN( select Warehouse_name ,DATE(order_date) ORDER_Date ,FORMAT_DATE(\'%b\', DATE(order_date)) AS month_name ,awb ,city ,state ,reference_code ,customer_name ,sum(suborder_quantity) Quantity ,SUM(selling_price) AS selling_price FROM maplemonk.sirona_wh_EasyEcom_FACT_ITEMS group by 1,2,3,4,5,6,7,8 ) b on a.PO_Number = b.reference_code left join( select Distinct Reference_Code, coalesce(ts.MP_Name,c.mp_name) MP_Name, Sub_Channel, Channel from maplemonk.easyecom_new_tax_sales ts left join (select distinct upper(mp_name) mp_name, upper(model_name) Sub_Channel, upper(channel) channel from maplemonk.googlesheet_marketplace_mapping) c on upper(ts.mp_name) = upper(c.mp_name) ) d on a.PO_Number = d.reference_code;",
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
            