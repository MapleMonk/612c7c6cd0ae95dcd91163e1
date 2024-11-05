{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE cp_2 as select * from ( SELECT *, CASE WHEN OUTFORDELIVERY_DATE > MAX_SLA THEN \'breach\' ELSE \'non-breach\' END AS breach_flag, (DATEDIFF(\'day\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) AS breach_date, CASE WHEN (DATEDIFF(\'day\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) < -2 THEN \'-3\' WHEN (DATEDIFF(\'day\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) = -2 THEN \'-2\' WHEN (DATEDIFF(\'day\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) = -1 THEN \'-1\' WHEN (DATEDIFF(\'day\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) = 0 THEN \'0\' WHEN (DATEDIFF(\'day\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) = 1 THEN \'1\' WHEN (DATEDIFF(\'day\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) = 2 THEN \'2\' ELSE \'3\' END AS breach_date_category, (DATEDIFF(\'hour\', OUTFORDELIVERY_DATE, MAX_SLA) * -1) AS breach_hours, DATEDIFF(\'hour\', PICKUP_DATE, OUTFORDELIVERY_DATE) AS actual_sla_shipping_hrs, DATEDIFF(\'day\', PICKUP_DATE, OUTFORDELIVERY_DATE) AS actual_sla_shipping_days, DATEDIFF(\'hour\', ORDER_DATE, OUTFORDELIVERY_DATE) AS actual_sla_delivery_hrs, DATEDIFF(\'day\', ORDER_DATE,OUTFORDELIVERY_DATE ) AS actual_sla_delivery_days, DATEDIFF(\'day\',PICKUP_DATE,MAX_SLA) AS predicted_tat FROM cp_1 where order_date > \'2024-07-01\' AND status = \'DELIVERED\' AND JOURNEY= \'Forward\' )A left join ( select * from ( SELECT ZONE AS Zone_m, City_Tier AS City_Tier, Delivery_Postcode, ROW_NUMBER() OVER (PARTITION BY Delivery_Postcode ORDER BY 1) AS rn FROM snitch_db.maplemonk.pincodemappingzoneupdatedsnitch ) WHERE rn=1) B on A.PINCODE = B.DELIVERY_POSTCODE",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            