{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table eggozdb.maplemonk.Complaint_dashboard as SELECT cast(timestampadd(minute,330,QC.complaint_date) as date) as Complaint_Date,QC.complaint_type,QC.purchase_from,Qc.complaint_source,QC.complaint_body,Qc.complaint_resolution,QC.resolution_status,Qc.customer_location,Qc.customer_name, QC.batch_id, BM.ID as Batchlink,BM.DATE,bm.procurement_id, PP.id as procuremntIDlink ,pp.farm_id, FF.id as farmidlink ,FF.FARM_NAME, CASE WHEN ff.FARM_NAME IN (\'Ajit Layer\',\'Bhairavnath Poultry Farm\') THEN \'West\' WHEN ff.FARM_NAME IN (\'MA Poulltry and Feedss\',\'Manikanta Farm\') THEN \'Hyderabad\' WHEN ff.FARM_NAME = \'NJ FOOD PRODUCTS\' THEN \'Chennai\' WHEN ff.FARM_NAME IN ( \'NJ FOODS PRODUCTS ( SS FARM )\', \'Rayudu Poultry farm\', \'Rayudu farm packing\', \'Sri Shyla Farm\' ) THEN \'Bangalore\' WHEN ff.FARM_NAME IN ( \'Ayanna Farms\',\'Choudhary Farm\',\'Dhanda Poultry Farm\',\'Farm delights\', \'GM Poultries\',\'JK Farm\',\'Kiwi Agro\',\'Kohinoor Poultry Farm\', \'M/s SANGWAN FARMS\',\'National Poultry\',\'Real Foods\', \'SHREE RADHEY RADHEY POULTRIES\',\'Smart Yield Agro\', \'Zamidara Farm\',\'kaypee poultry farm\',\'sky layer farm\' ) THEN \'North\' ELSE \'Other\' END AS Area FROM my_sql_procurement_qccomplaint as QC LEFT JOIN my_sql_procurement_batchmodel as BM ON QC.BATCH_id = BM.id LEFT JOIN my_sql_procurement_procurement as PP ON BM.procurement_id = PP.id LEFT JOIN my_sql_farmer_farm As FF ON PP.farm_id = FF.id;",
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
            