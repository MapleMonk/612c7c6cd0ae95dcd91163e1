{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.zouk_qc_actual_vs_target AS WITH TARGETS AS ( WITH parsed_dates AS ( SELECT PARSE_DATE(\'%d-%b-%Y\', Start_Date) AS Start_Date, PARSE_DATE(\'%d-%b-%Y\', End_Date) AS End_Date, CAST(Target AS FLOAT64) AS Target, UPPER(Category) AS Category, UPPER(QC_Category_Bucket) AS QC_Category_Bucket FROM maplemonk.qc_target qualify row_number() over(partition by Start_Date,End_Date,upper(Category),lower(QC_Category_Bucket) order by 1) = 1 ), date_series AS ( SELECT Start_Date, End_Date, Target, Category, QC_Category_Bucket, DATE_ADD(Start_Date, INTERVAL day_offset DAY) AS daily_date FROM parsed_dates, UNNEST(GENERATE_ARRAY(0, DATE_DIFF(End_Date, Start_Date, DAY))) AS day_offset ) SELECT daily_date AS Date, Target, Category, QC_Category_Bucket FROM date_series ORDER BY Date ), qc_data AS ( SELECT PARSE_DATE(\'%d-%b-%y\', Date) AS Date, UPPER(QC_Team_Member) AS QC_Team_Member, UPPER(fi.Category) AS Category, SUM(SAFE_CAST(QC_Accepted AS INT64)) AS QC_Accepted, SUM(SAFE_CAST(QC_Damaged AS INT64)) AS QC_Damaged FROM maplemonk.qc_processing_data fi LEFT JOIN ( SELECT * FROM ( SELECT category, sub_category, category_code, collection, PRODUCT_TYPE, BAU_OFFLINE, BAU_ONLINE, TAX_RATE, ROW_NUMBER() OVER (PARTITION BY category ORDER BY 1) rw FROM zouk-wh.maplemonk.final_sku_master ) WHERE rw = 1 ) pid ON LOWER(fi.category) = LOWER(pid.category) WHERE Date IS NOT NULL AND Date != \'#REF!\' GROUP BY 1, 2, 3 ) SELECT COALESCE(qc.Date, tar.Date) AS Date, UPPER(COALESCE(qc.Category, tar.Category)) AS Category, qc.QC_Team_Member, IFNULL(qc.QC_Accepted, 0) AS QC_Accepted, IFNULL(qc.QC_Damaged, 0) AS QC_Damaged, IFNULL(tar.Target, 0) AS Target FROM qc_data qc left JOIN TARGETS tar ON LOWER(qc.Category) = LOWER(tar.Category) and qc.Date = tar.Date;",
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
            