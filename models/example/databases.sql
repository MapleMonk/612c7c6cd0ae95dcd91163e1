{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT * FROM my_sql_procurement_qccomplaint LEFT JOIN my_sql_procurement_batchmodel ON my_sql_procurement_qccomplaint.BATCH_id = my_sql_procurement_batchmodel.id LEFT JOIN my_sql_procurement_procurement ON my_sql_procurement_batchmodel.procurement_id = my_sql_procurement_procurement.id LEFT JOIN my_sql_farmer_farm ON my_sql_procurement_procurement.farm_id = my_sql_farmer_farm.id;",
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
            