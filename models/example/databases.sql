{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or Replace table snitch_db.maplemonk.final_fcatory_mapping as SELECT _AIRBYTE_DATA:Capacity::STRING AS Capacity, _AIRBYTE_DATA:Factory::STRING AS Factory, _AIRBYTE_DATA:\"Factory_Code \"::STRING AS Factory_Code FROM snitch_db.maplemonk._airbyte_raw_new_job_factory_mapping_; Create or Replace table snitch_db.maplemonk.production_data as With production_data as ( Select Adjusted_SKU,sku,Type,mrp, CATEGORY, order_type, Status, Production_stage, \"FI_STATUS\", \"HSN_Code \", fabric_code, factory_name, AGEING, Incharg_Name, Responsibility, Issued_date, EXPECTED_DELIVERY_DATE, CASE WHEN COALESCE(revised_delivery_date, EXPECTED_DELIVERY_DATE) IS NULL THEN case when lower(order_type)=\'fob\' then issued_date + 90 else issued_date + 60 end ELSE COALESCE(revised_delivery_date, EXPECTED_DELIVERY_DATE) END AS EXPECTED_DELIVERY_DATE_cal, proj_qty, cut_qty, \"RTS QTY\", expected_pps, REVISED_DELIVERY_DATE, PSS_DATE, FI_DATE, Inward_date, REMARKS from ( SELECT CASE WHEN LEFT(SKU, 1) = \'R\' THEN RIGHT(SKU, LENGTH(SKU) - 1) ELSE SKU END AS Adjusted_SKU, sku, CASE WHEN LEFT(SKU, 1) = \'R\' THEN \'Repeat\' ELSE \'New\' END AS Type, mrp, CATEGORY, order_type, sku_status_ AS Status, sku_status AS Production_stage, \"FI_STATUS\", \"HSN_Code \", fabric_code, Factory_Name as factory_name, DATEDIFF(\'day\', CASE WHEN REGEXP_LIKE(date_issued, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN TO_DATE(REPLACE(date_issued, \'/\', \'-\'), \'DD-MM-YYYY\') ELSE NULL END, CURRENT_DATE ) AS AGEING, Incharg_Name, CASE WHEN INCHARG_NAME IN (\'Khushal_J\', \'Khushal_R\') THEN \'Khushal\' when lower( Incharg_Name) like \'%montu%\' THEN \'Montu\' ELSE Incharg_Name END AS Responsibility, CASE WHEN REGEXP_LIKE(date_issued, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(date_issued, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS Issued_date, CASE WHEN REGEXP_LIKE(EXPECTED_DELIVERY_DATE, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(EXPECTED_DELIVERY_DATE, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS EXPECTED_DELIVERY_DATE, cutting_ratio, fabric_meters, fabric_vendor, avg_cons, work_order_link, COALESCE(TRY_CAST(proj_qty AS NUMBER), 0) - COALESCE(TRY_CAST(b2b_qty AS NUMBER), 0) AS proj_qty, COALESCE(TRY_CAST(cut_qty AS NUMBER), 0) AS cut_qty, COALESCE(TRY_CAST(RTS_QTY AS NUMBER), 0) AS \"RTS QTY\", CASE WHEN REGEXP_LIKE(expected_pps, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(expected_pps, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS expected_pps, CASE WHEN REGEXP_LIKE(REVISED_DELIVERY_DATE, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(REVISED_DELIVERY_DATE, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS REVISED_DELIVERY_DATE, CASE WHEN REGEXP_LIKE(\"PSS DATE\", \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(\"PSS DATE\", \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS PSS_DATE, CASE WHEN REGEXP_LIKE(FI_DATE, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(FI_DATE, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS FI_DATE, CASE WHEN REGEXP_LIKE(\"Inward_date \", \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(\"Inward_date \", \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS Inward_date, REMARKS FROM snitch_db.maplemonk.WORK_ORDERS_final_NEW_MAIN WHERE sku IS NOT NULL) ), factory as ( Select CAST(Capacity AS INT) AS Capacity, factory from ( SELECT Capacity, factory, ROW_NUMBER() OVER (PARTITION BY factory ORDER BY Capacity DESC) AS rn FROM snitch_db.maplemonk.final_fcatory_mapping ) where rn=1) SELECT a.*, b.Capacity from production_data a left join factory b on a.factory_name = b.factory; Create or Replace table snitch_db.maplemonk.production_data_kpi as WITH production_data AS ( SELECT sku, CASE WHEN REGEXP_LIKE(date_issued, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(date_issued, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS Issued_date, CASE WHEN Incharg_Name IN (\'Khushal_J\', \'Khushal_R\') THEN \'Khushal\' WHEN LOWER(INCHARG_NAME) LIKE \'%montu%\' THEN \'Montu\' ELSE Incharg_Name END AS Responsibility, Factory_Name AS factory_name, CASE WHEN REGEXP_LIKE(EXPECTED_DELIVERY_DATE, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(EXPECTED_DELIVERY_DATE, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS EXPECTED_DELIVERY_DATE, CASE WHEN REGEXP_LIKE(REVISED_DELIVERY_DATE, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(REVISED_DELIVERY_DATE, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS REVISED_DELIVERY_DATE, COALESCE(TRY_CAST(proj_qty AS NUMBER), 0) - COALESCE(TRY_CAST(b2b_qty AS NUMBER), 0) AS proj_qty , COALESCE(TRY_CAST(cut_qty AS NUMBER), 0) AS cut_qty, sku_status_ AS Status, CASE WHEN LEFT(SKU, 1) = \'R\' THEN \'Repeat\' ELSE \'New\' END AS Type, CATEGORY, order_type FROM snitch_db.maplemonk.WORK_ORDERS_final_NEW_MAIN WHERE LOWER(status) IN (\'active\', \'delivered\') ), Fi_data AS ( SELECT sku, CASE WHEN REGEXP_LIKE(date_issued, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(date_issued, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS Issued_date, CASE WHEN Incharg_Name IN (\'Khushal_J\', \'Khushal_R\') THEN \'Khushal\' WHEN LOWER(INCHARG_NAME) LIKE \'%montu%\' THEN \'Montu\' ELSE Incharg_Name END AS Responsibility, Factory_Name AS factory_name, CASE WHEN REGEXP_LIKE(FI_DATE, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(FI_DATE, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS FI_DATE, CASE WHEN REGEXP_LIKE(expected_fi_date, \'^[0-9]{2}[-/][0-9]{2}[-/][0-9]{4}$\') THEN DATE_TRUNC(\'Day\', TO_DATE(REPLACE(expected_fi_date, \'/\', \'-\'), \'DD-MM-YYYY\')) ELSE NULL END AS expected_fi_date, COALESCE(TRY_CAST(RTS_QTY AS NUMBER), 0) AS \"RTS QTY\", sku_status_ AS Status, CASE WHEN LEFT(SKU, 1) = \'R\' THEN \'Repeat\' ELSE \'New\' END AS Type, CATEGORY, order_type FROM snitch_db.maplemonk.WORK_ORDERS_final_NEW_MAIN WHERE expected_fi_date IS NOT NULL or fi_date is not null ) -- Full Outer Join using SKU and Issued_date SELECT COALESCE(p.sku, f.sku) AS sku, CASE WHEN LEFT( COALESCE(p.sku, f.sku), 1) = \'R\' THEN RIGHT( COALESCE(p.sku, f.sku), LENGTH(COALESCE(p.sku, f.sku)) - 1) ELSE COALESCE(p.sku, f.sku) END AS Adjusted_SKU, COALESCE(p.Issued_date, f.Issued_date) AS Issued_date, COALESCE(f.expected_fi_date,f.FI_DATE, p.REVISED_DELIVERY_DATE, p.EXPECTED_DELIVERY_DATE) AS Delivery_Date, COALESCE(p.Responsibility, f.Responsibility) AS Responsibility, COALESCE(p.factory_name, f.factory_name) AS factory_name, f.expected_fi_date, f.FI_DATE, p.proj_qty, p.cut_qty, f.\"RTS QTY\", p.Status, p.Type, p.CATEGORY, p.order_type, case when f.FI_DATE is null then null else datediff(\'Day\',p.Issued_date, f.FI_DATE) end as lead_time FROM production_data p FULL OUTER JOIN Fi_data f ON p.sku = f.sku AND p.Issued_date = f.Issued_date;",
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
            