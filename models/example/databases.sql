{{ config(
            materialized='table',
                post_hook={
                    "sql": "INSERT INTO snitch_db.maplemonk.offline_master_Daily_Report_1 (\"SKU_GROUP\", \"BRANCH_CODE\", \"MARKETPLACE_MAPPED\", \"FIRST_ORDER_DATE\", \"TOTAL_SALES\", \"TOTAL_RETURNS\", \"DAYS_SINCE_FIRST_ORDER\", \"AVERAGE_RETURN_SINCE_FIRST_ORDER\", \"NATURAL_ROS\", \"FINAL_ROS\", \"SALES_FIRST_7_DAYS\", \"SALES_FIRST_15_DAYS\", \"SALES_FIRST_30_DAYS\", \"SALES_LAST_7_DAYS\", \"SALES_LAST_15_DAYS\", \"SALES_LAST_30_DAYS\", \"INVENTORY\", \"XS_UNITS\", \"S_UNITS\", \"M_UNITS\", \"L_UNITS\", \"XL_UNITS\", \"XXL_UNITS\", \"XL3_UNITS\", \"XL4_UNITS\", \"XL5_UNITS\", \"XL6_UNITS\", \"NUM_SIZE_AVAILABLE\", \"FRESH_INVENTORY\", \"JIT_XS_UNITS\", \"JIT_S_UNITS\", \"JIT_M_UNITS\", \"JIT_L_UNITS\", \"JIT_XL_UNITS\", \"JIT_2XL_UNITS\", \"JIT_3XL_UNITS\", \"JIT_4XL_UNITS\", \"JIT_5XL_UNITS\", \"JIT_6XL_UNITS\", \"JIT_QTY\", \"CUT_SIZE_WITH_JIT_FLAG\", \"CUT_SIZE_FLAG\", \"DESIGNS\", \"NEW_CATEGORY\", \"COLLAR_NEW\", \"MATERIAL_NEW\", \"OCCASSION_NEW\", \"PRINT_DESIGN\", \"SLEEVE_TYPE\", \"FIT\", \"STYLE\", \"COLOR\", \"CUMULATIVE_SHARE\", \"PERCENTAGE_CATEGORY\", \"SKU_CLASS\", \"ONLINE_FINAL_ROS\", \"ONLINE_NATURAL_ROS\", \"CLUSTER\", \"BOH_AREA\", \"BASE_STOCK\", \"CARPET_AREA\", \"VM_OPTION\", \"DEPT_IDEAL\", \"AM\", \"RM\", \"NSM\", \"CITY\", \"STATE\", \"REGION\", \"PARTNER\", \"DATE\") SELECT \"SKU_GROUP\", \"BRANCH_CODE\", \"MARKETPLACE_MAPPED\", \"FIRST_ORDER_DATE\", \"TOTAL_SALES\", \"TOTAL_RETURNS\", \"DAYS_SINCE_FIRST_ORDER\", \"AVERAGE_RETURN_SINCE_FIRST_ORDER\", \"NATURAL_ROS\", \"FINAL_ROS\", \"SALES_FIRST_7_DAYS\", \"SALES_FIRST_15_DAYS\", \"SALES_FIRST_30_DAYS\", \"SALES_LAST_7_DAYS\", \"SALES_LAST_15_DAYS\", \"SALES_LAST_30_DAYS\", \"INVENTORY\", \"XS_UNITS\", \"S_UNITS\", \"M_UNITS\", \"L_UNITS\", \"XL_UNITS\", \"XXL_UNITS\", \"XL3_UNITS\", \"XL4_UNITS\", \"XL5_UNITS\", \"XL6_UNITS\", \"NUM_SIZE_AVAILABLE\", \"FRESH_INVENTORY\", \"JIT_XS_UNITS\", \"JIT_S_UNITS\", \"JIT_M_UNITS\", \"JIT_L_UNITS\", \"JIT_XL_UNITS\", \"JIT_2XL_UNITS\", \"JIT_3XL_UNITS\", \"JIT_4XL_UNITS\", \"JIT_5XL_UNITS\", \"JIT_6XL_UNITS\", \"JIT_QTY\", \"CUT_SIZE_WITH_JIT_FLAG\", \"CUT_SIZE_FLAG\", \"DESIGNS\", \"NEW_CATEGORY\", \"COLLAR_NEW\", \"MATERIAL_NEW\", \"OCCASSION_NEW\", \"PRINT_DESIGN\", \"SLEEVE_TYPE\", \"FIT\", \"STYLE\", \"COLOR\", \"CUMULATIVE_SHARE\", \"PERCENTAGE_CATEGORY\", \"SKU_CLASS\", \"ONLINE_FINAL_ROS\", \"ONLINE_NATURAL_ROS\", \"CLUSTER\", \"BOH_AREA\", \"BASE_STOCK\", \"CARPET_AREA\", \"VM_OPTION\", \"DEPT_IDEAL\", \"AM\", \"RM\", \"NSM\", \"CITY\", \"STATE\", \"REGION\", \"PARTNER\", current_date as date FROM snitch_db.maplemonk.offline_master where not exists ( SELECT 1 AS Exist FROM snitch_db.maplemonk.offline_master_Daily_Report_1 WHERE DATE = CURRENT_DATE() LIMIT 1) ;",
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
            