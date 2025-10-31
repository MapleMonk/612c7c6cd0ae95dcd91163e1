{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table eggozdb.maplemonk.Customer_dashboard as SELECT QC.complaint_date,QC.complaint_type,QC.purchase_from,Qc.complaint_source,QC.complaint_body,Qc.complaint_resolution,QC.resolution_status,Qc.customer_location,Qc.customer_name, QC.batch_id, BM.ID as Batchlink,BM.DATE,bm.procurement_id, PP.id as procuremntIDlink ,pp.farm_id, FF.id as farmidlink ,FF.FARM_NAME FROM my_sql_procurement_qccomplaint as QC LEFT JOIN my_sql_procurement_batchmodel as BM ON QC.BATCH_id = BM.id LEFT JOIN my_sql_procurement_procurement as PP ON BM.procurement_id = PP.id LEFT JOIN my_sql_farmer_farm As FF ON PP.farm_id = FF.id;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            