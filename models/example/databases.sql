{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.EOQ_TABLE_V2 AS SELECT eoq.*, shipped_eoq.shipped_from_factory, shipped_eoq.expected_del_date AS shipped_expected_del_date, shipped_eoq.dispatched_qty, FACTORY_UP_YTBC_EOQ.factory AS YTBC_factory, FACTORY_UP_YTBC_EOQ.expected_del_date AS YTBC_expected_del_date, FACTORY_UP_YTBC_EOQ.cut_qty AS YTBC_cut_qty, FACTORY_UP_YTBC_EOQ.Status AS YTBC_status FROM snitch_db.maplemonk.eoq LEFT JOIN ( SELECT sku_group, factory AS shipped_from_factory, expected_del_date, cut_qty AS dispatched_qty FROM snitch_db.maplemonk.factory_shipped_eoq WHERE expected_del_date > CURRENT_DATE() AND expected_del_date < DATEADD(\'day\', 90, CURRENT_DATE()) ) shipped_eoq ON eoq.sku_group = shipped_eoq.sku_group LEFT JOIN ( SELECT sku_group, factory, expected_del_date, cut_qty, \'YTBC\' AS Status FROM snitch_db.maplemonk.FACTORY_UP_YTBC_EOQ ) AS factory_up_ytbc_eoq ON eoq.sku_group = factory_up_ytbc_eoq.sku_group; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.UNDER_PRODUCTION AS SELECT SKU_NUMBER, FACTORY_, \"Fabric Code\", CUTTING_RATIO, MRP, FABRIC_VENDOR_CODE, EXPECTED_DELIVERY_DATE, DATE_ISSUED, COUNT(CASE WHEN TRY_CAST(cut_qty AS INT) IS NULL THEN sku_number END) AS Yet_to_be_cut, CASE WHEN COUNT(CASE WHEN TRY_CAST(cut_qty AS INT) IS NULL THEN sku_number END) = 1 THEN \'YTBC\' ELSE \'DONE\' END AS STATUS, SUM(CUT_QTY) AS sum_cut_qty, DATEDIFF(\'day\', DATE_ISSUED, CURRENT_DATE()) AS AGING, CASE WHEN UPPER(SUBSTR(SKU_NUMBER, 1, 1)) = \'R\' THEN \'Repeat\' ELSE \'New\' END AS Type FROM snitch_db.maplemonk.WORK_ORDERS_HIDDEN_MAIN GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;",
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
                        