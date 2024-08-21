{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table snitch_db.maplemonk.Fabric_orders as SELECT \'PF\' AS Vendor, \"SKU name\", LINK, METERS, \"DC COPY\" AS \"DC COPY\", FACTORY, \"RATE/MTR\" AS \"RATE/MTR\", \"BILL COPY\" AS \"BILL COPY\", \"LEAD TIME\" AS \"LEAD TIME\", DISPATCHED, \"SNITCH SKU\" AS \"SNITCH SKU\", to_date(\"Order Placed\",\'DD/MM/YYYY\') AS \"Order Placed\", CONFIRMATION AS CONFIRMATION, \"FABRIC DESIGN\" AS \"FABRIC DESIGN\", \"dispatch ready\" AS \"Goods Ready\" FROM snitch_db.maplemonk.consol_pf where \"Order Placed\" != \'Order Placed\' UNION SELECT \'Artex\' AS Vendor, \"SKU name\", LINK, METERS, \"DC COPY\" AS \"DC COPY\", FACTORY, \"RATE/MTR\" AS \"RATE/MTR\", \"BILL COPY\" AS \"BILL COPY\", \"LEAD TIME\" AS \"LEAD TIME\", DISPATCHED, \"SNITCH SKU\" AS \"SNITCH SKU\", to_date(\"Order Placed\",\'DD/MM/YYYY\') AS \"Order Placed\", \"Order Confirmation\" AS CONFIRMATION, \"FABRIC DESIGN\" AS \"FABRIC DESIGN\", \"Goods Ready\" AS \"Goods Ready\" FROM snitch_db.maplemonk.CONSOL_ARTEX where \"Order Placed\" != \'Order Placed\'; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.EOQ_TABLE_V3 AS SELECT EOQ_Test.*, shipped_eoq.shipped_from_factory, shipped_eoq.expected_del_date AS shipped_expected_del_date, shipped_eoq.dispatched_qty, factory_up_ytbc_eoq.YTBC_factory, factory_up_ytbc_eoq.YTBC_expected_del_date, factory_up_ytbc_eoq.YTBC_cut_qty, factory_up_ytbc_eoq.YTBC_status, Fabric_orders.Vendor as Mill, Fabric_orders.\"Order Placed\", Fabric_orders.CONFIRMATION, Fabric_orders.METERS, CASE WHEN LOWER(Fabric_orders.CONFIRMATION) = \'no\' THEN \'Fabric not Available\' WHEN LOWER(Fabric_orders.CONFIRMATION) = \'yes\' THEN \'Fabric Order Placed & Confirmed\' WHEN Fabric_orders.CONFIRMATION IS NULL THEN \'Fabric Order Yet to be Placed\' END AS Fabric_Status, BEST_CHANNELS.\"Best Seller Channel\", FACTORY_SUGGESTION.factory_ AS FACTORY_SUGGESTION, Under_production_flag.QUANTITY as QUP_FLAG_1, Under_production_flag.EXPECTED_DELIVERY_DATE AS QUP_EDD FROM snitch_db.maplemonk.EOQ_Test LEFT JOIN ( Select \"SKU_NO.\" as Sku_group, factory as shipped_from_factory, expected_del_date , total_online_qty as dispatched_qty from ( Select \"SKU_NO.\",factory,total_online_qty, CASE WHEN delivery_planned_date LIKE \'__-__-____\' THEN TO_DATE(delivery_planned_date, \'DD-MM-YYYY\') WHEN delivery_planned_date LIKE \'__/__/____\' THEN TO_DATE(delivery_planned_date, \'DD/MM/YYYY\') ELSE NULL END as expected_del_date from snitch_db.maplemonk.hsk_inventory where STATUS IS NULL UNION ALL SELECT \"SKU_NO.\", factory, total_online_qty, CASE WHEN delivery_planned_date LIKE \'__-__-____\' THEN TO_DATE(delivery_planned_date, \'DD-MM-YYYY\') WHEN delivery_planned_date LIKE \'__/__/____\' THEN TO_DATE(delivery_planned_date, \'DD/MM/YYYY\') ELSE NULL END AS expected_del_date FROM snitch_db.maplemonk.YLK_INVENTORY WHERE LOWER(put_away_date) NOT IN (\'cleared\', \'cleared \') OR put_away_date IS NULL UNION ALL Select \"SKU_NO.\",factory,total_online_qty, CASE WHEN delivery_planned_date LIKE \'__-__-____\' THEN TO_DATE(delivery_planned_date, \'DD-MM-YYYY\') WHEN delivery_planned_date LIKE \'__/__/____\' THEN TO_DATE(delivery_planned_date, \'DD/MM/YYYY\') ELSE NULL END as expected_del_date from snitch_db.maplemonk.EMIZA_INVENTORY where REMARK IS NULL ) ) shipped_eoq ON EOQ_Test.sku_group = shipped_eoq.Sku_group LEFT JOIN ( SELECT SKU_GROUP AS sku_group, FACTORY AS YTBC_factory, expected_del_date AS YTBC_expected_del_date, CUT_QTY AS YTBC_cut_qty, Status AS YTBC_status FROM ( SELECT SKU_GROUP, FACTORY, CASE WHEN TRY_TO_DATE(EXPECTED_DEL_DATE, \'YYYY-MM-DD\') IS NOT NULL THEN TO_DATE(EXPECTED_DEL_DATE, \'YYYY-MM-DD\') WHEN TRY_TO_DATE(EXPECTED_DEL_DATE, \'DD-MM-YYYY\') IS NOT NULL THEN TO_DATE(EXPECTED_DEL_DATE, \'DD-MM-YYYY\') ELSE NULL END AS expected_del_date, CUT_QTY, \'YTBC\' AS Status, FROM snitch_db.maplemonk.factory_up_ytbc_eoq) GROUP BY 1,2,3,4,5 ) factory_up_ytbc_eoq ON EOQ_Test.sku_group = factory_up_ytbc_eoq.sku_group LEFT JOIN ( SELECT \"SNITCH SKU\", Vendor, \"Order Placed\", CONFIRMATION, METERS FROM snitch_db.maplemonk.Fabric_orders ) Fabric_orders ON EOQ_Test.sku_group = Fabric_orders.\"SNITCH SKU\" LEFT JOIN ( SELECT SKU_GROUP AS sku_group, MARKETPLACE_MAPPED AS \"Best Seller Channel\" FROM ( SELECT SKU_GROUP, MARKETPLACE_MAPPED, SKU_CLASS, ROUND(SUM(CASE WHEN MARKETPLACE_MAPPED = \'AJIO\' THEN (SELLING_PRICE * 1.5) ELSE SELLING_PRICE END) / 100, 0) AS PRICE_POINT, SUM(CASE WHEN MARKETPLACE_MAPPED = \'AJIO\' THEN (SELLING_PRICE * 1.5) ELSE SELLING_PRICE END) AS SELLING_PRICE_TOTAL, SUM(NULLIF(SHIPPING_QUANTITY, 0)) AS total_quantity, ROUND(SUM(NULLIF(SHIPPING_QUANTITY, 0)) / 10, 0) AS QUANTITY_POINT, MAX(CASE WHEN MARKETPLACE_MAPPED = \'AJIO\' THEN (SELLING_PRICE * 1.5) ELSE SELLING_PRICE END) AS MAX, MIN(CASE WHEN MARKETPLACE_MAPPED = \'AJIO\' THEN (SELLING_PRICE * 1.5) ELSE SELLING_PRICE END) AS MIN, AVG(CASE WHEN MARKETPLACE_MAPPED = \'AJIO\' THEN (SELLING_PRICE * 1.5) ELSE SELLING_PRICE END / NULLIF(SHIPPING_QUANTITY, 0)) AS average_asp, ROUND((AVG(CASE WHEN MARKETPLACE_MAPPED = \'AJIO\' THEN (SELLING_PRICE * 1.5) ELSE SELLING_PRICE END / NULLIF(SHIPPING_QUANTITY, 0))) / 100, 0) AS average_asp_POINT, ROW_NUMBER() OVER (PARTITION BY SKU_GROUP ORDER BY QUANTITY_POINT DESC, average_asp_POINT DESC) AS marketplace_rank, MAX(MRP) AS MRP FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE ORDER_STATUS = \'COMPLETE\' and MARKETPLACE_MAPPED not in (\'OWN_STORE\',\'FRANCHISE_STORE\') AND ORDER_DATE > DATEADD(DAY, -120, CURRENT_DATE()) AND SELLING_PRICE > 0 GROUP BY SKU_GROUP, MARKETPLACE_MAPPED, SKU_CLASS ) sub WHERE marketplace_rank = 1 ) BEST_CHANNELS ON EOQ_Test.sku_group = BEST_CHANNELS.sku_group LEFT JOIN ( Select adjusted_sku,factory_ from ( SELECT adjusted_sku, factory_, ROW_NUMBER() OVER (PARTITION BY ADJUSTED_SKU ORDER BY DATE DESC) AS RN FROM ( SELECT CASE WHEN \"SKU Number\" LIKE \'R%\' THEN SUBSTR(\"SKU Number\", 2) ELSE \"SKU Number\" END AS adjusted_sku, case when \"Factory Vendor Code\" in (\'01 - SSTC\', \'03 - Leandro\', \'13 - Kharva\', \'18 - Expert\', \'16 - Thandeshwara\', \'09 - Nalanda\', \'05 - Sai Textiles\', \'06 - Alagesh\', \'MQ - M Square\', \'14 - Lohith Creations\', \'IN - Mohit\', \'19 - S R Apparels\', \'Navkar\', \'02 - Kasturi Creation\', \'08 - Vignesh\', \'21-BLUE MONKEY\', \'KU - Kunal\', \'AC - Asian Clothing\', \'07 - K P\', \'15 - Prestige\', \'VN - Vanitha Garments\', \'10 - Adeep \') then \"Factory Vendor Code\" else factory end as factory_, TO_DATE(\"Date Issued\", \'DD-MM-YYYY\') AS DATE FROM snitch_db.maplemonk.PRODUCT_TRACKING_HIDDEN_SHIPPED UNION ALL Select CASE WHEN sku LIKE \'R%\' THEN SUBSTR(sku, 2) ELSE sku END AS adjusted_sku, factory_name, to_date(date_issued,\'DD-MM-YYYY\') AS DATE, from snitch_db.maplemonk.work_orders_new_main WHERE SKU_STATUS_ =\'Delivered\' and date_issued !=\'Delivered\') ) sub WHERE RN = 1 ) FACTORY_SUGGESTION ON EOQ_Test.sku_group = FACTORY_SUGGESTION.adjusted_sku Left join ( Select SKU_GROUP, QUANTITY, EXPECTED_DELIVERY_DATE FROM snitch_db.MAPLEMONK.FACTORY_PRODUCTION_INVENTORY) Under_production_flag on EOQ_Test.sku_group = Under_production_flag.SKU_GROUP;",
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
            