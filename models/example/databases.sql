{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.Global_pareto_v2_Daily_Report_1 AS WITH DateCheck AS ( SELECT 1 AS Exist FROM snitch_db.maplemonk.Global_pareto_v2_Daily_Report_1 WHERE DATE = CURRENT_DATE() LIMIT 1 ) SELECT SKU_GROUP ,NEW_CATEGORY ,STYLE ,DESIGNS ,COLLAR_NEW ,MATERIAL_NEW ,SLEEVE_TYPE ,FIT ,COLOR ,SKU_CLASS ,CLUSTER_COUNT ,CLUSTERS_LIST ,MISSING_CLUSTERS ,MISSING_BRANCH_CODES ,MISSING_BRANCH_CODE_COUNT ,OFFLINE_INVENTORY ,WH_INVENTORY ,XS_UNITS ,S_UNITS ,M_UNITS ,L_UNITS ,XL_UNITS ,TOTAL_SALES_30D ,SHARE ,CUMULATIVE_SHARE ,PERCENTAGE_CATEGORY ,SIZE_SET_COUNT ,REMARKS ,current_date as date FROM snitch_db.maplemonk.Global_pareto_v2 WHEre not EXISTS (SELECT * FROM DateCheck) union all select SKU_GROUP ,NEW_CATEGORY ,STYLE ,DESIGNS ,COLLAR_NEW ,MATERIAL_NEW ,SLEEVE_TYPE ,FIT ,COLOR ,SKU_CLASS ,CLUSTER_COUNT ,CLUSTERS_LIST ,MISSING_CLUSTERS ,MISSING_BRANCH_CODES ,MISSING_BRANCH_CODE_COUNT ,OFFLINE_INVENTORY ,WH_INVENTORY ,XS_UNITS ,S_UNITS ,M_UNITS ,L_UNITS ,XL_UNITS ,TOTAL_SALES_30D ,SHARE ,CUMULATIVE_SHARE ,PERCENTAGE_CATEGORY ,SIZE_SET_COUNT ,REMARKS ,DATE FROM snitch_db.maplemonk.Global_pareto_v2_Daily_Report_1;",
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
            