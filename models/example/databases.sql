{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table snitch_db.maplemonk.one_inventory_view_erp as Select * from snitch_db.maplemonk.logicerp_warehouse_get_stock_in_hand where CONVERT_TIMEZONE(\'UTC\', \'Asia/Kolkata\', date::DATETIME)::date = current_date; Create or replace table snitch_db.maplemonk.one_inventory_view_erp_consol as Select BRANCH_CODE,BRANCH_NAME,GODOWN_NAME ,ADDLITEMCODE, sum(stock_qty) as qty from snitch_db.maplemonk.logicerp_warehouse_get_stock_in_hand where CONVERT_TIMEZONE(\'UTC\', \'Asia/Kolkata\', date::DATETIME)::date = current_date group by 1,2,3,4 Union all SELECT branch_code, branch_name, \'GOOD_INVENTORY\' as GODOWN_NAME, LOGICUSERCODE, SUM(STOCK_QTY) AS inventory, FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand WHERE LOGICUSERCODE NOT LIKE \'CB%\' AND DATE = CURRENT_DATE GROUP BY 1,2 ,4 UNION ALL Select * from ( With Over as ( sELECT BRANCH_CODE_PRIORITY , \"BRANCH NAME\", STATUS, \"ITEM CODE\", SUM(QTY) AS QTY FROM ( Select branch_code_priority , \"BRANCH NAME\", \'PUV Verification Pending\' as Status, \"ITEM CODE\", SUM(\"PUR QTY\") AS qty from ( SELECT a.*, b.branch_code_priority FROM snitch_db.maplemonk.final_puv_verification a left join ( SELECT DISTINCT e.\"BRANCH NAME\", s.branch_code_priority FROM snitch_db.maplemonk.final_puv_verification e LEFT JOIN snitch_db.maplemonk.storepriority s ON e.\"BRANCH NAME\" LIKE \'%\' || s.branch_name_priority || \'%\') b on a.\"BRANCH NAME\" =b.\"BRANCH NAME\" where \"GOODS IN TRANSIT\" = \'True\') GROUP BY 1,2,3,4 UNION ALL Select branch_code_priority , \"EXPORT PARTY NAME\", \'IN TRANSIT\' AS STATUS, \"EXPORT ADDITIONAL ITEM CODE\", SUM(\"EXPORT QUANTITY\") AS QTY_JIT, from ( SELECT a.*, b.branch_code_priority FROM snitch_db.maplemonk.offline_jit a left join ( SELECT DISTINCT e.\"EXPORT PARTY NAME\", s.branch_code_priority FROM snitch_db.maplemonk.offline_jit e LEFT JOIN snitch_db.maplemonk.storepriority s ON e.\"EXPORT PARTY NAME\" LIKE \'%\' || s.branch_name_priority || \'%\') b on a.\"EXPORT PARTY NAME\" =b.\"EXPORT PARTY NAME\" where \"IMPORT QUANTITY\" =\'\' or \"IMPORT QUANTITY\" is null) GROUP BY 1,2,3,4 ) GROUP BY 1,2,3,4 ), details as ( Select * from ( SELECT DISTINCT branch_code, marketplace_mapped, ROW_NUMBER() OVER (PARTITION BY branch_code ORDER BY order_date DESC) AS rn FROM snitch_db.maplemonk.STORE_fact_items_offline) WHERE rn = 1 ) Select a.BRANCH_CODE_PRIORITY, coalesce(b.marketplace_mapped,a.\"BRANCH NAME\") as Store , A.STATUS, A.\"ITEM CODE\", A.QTY from Over a left join details b on a.branch_code_priority = b.branch_code)",
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
            