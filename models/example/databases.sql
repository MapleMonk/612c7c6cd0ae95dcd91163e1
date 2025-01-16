{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE rpsg_db.maplemonk.DRV_RETENTION_PRODUCT AS SELECT DATE_TRUNC(\'DAY\', \"DATE\") AS DATE, CASE WHEN upper(PRODUCT) IN ( \'ACV EFFERVESCENT\', \'ACV JUICE\', \'ACV PRO\', \'DIABEX JUICE\', \'GUT CARE EFFERVESCENT\', \'HERBOSLIM\', \'LIVER CARE\', \'LIVER CARE TABS\', \'LIVER CARE TABLETS\', \'LIVER RESTORE\', \'PILES CARE\', \'PILES SPRAY\', \'PILES SPRAY COMBO\', \'PILOCARE JUICE\', \'SHILAJIT GOLD\', \'SHILAJIT GOLD COMBO\', \'SOFTGEL\', \'SOFTGEL GOLD\' ) THEN CASE WHEN upper(PRODUCT) = \'LIVER CARE TABLETS\' THEN \'LIVER CARE TABS\' ELSE PRODUCT END ELSE \'OTHER\' END AS PRODUCT, SUM(IFNULL(spend, 0)) AS Spend, SUM(IFNULL(REALISED_REVENUE, 0)) AS Delivered_Revenue FROM rpsg_db.maplemonk.drv_product_roas WHERE UPPER(CHANNEL) = \'RETENTION\' GROUP BY DATE_TRUNC(\'DAY\', \"DATE\"), CASE WHEN upper(PRODUCT) IN ( \'ACV EFFERVESCENT\', \'ACV JUICE\', \'ACV PRO\', \'DIABEX JUICE\', \'GUT CARE EFFERVESCENT\', \'HERBOSLIM\', \'LIVER CARE\', \'LIVER CARE TABS\', \'LIVER CARE TABLETS\', \'LIVER RESTORE\', \'PILES CARE\', \'PILES SPRAY\', \'PILES SPRAY COMBO\', \'PILOCARE JUICE\', \'SHILAJIT GOLD\', \'SHILAJIT GOLD COMBO\', \'SOFTGEL\', \'SOFTGEL GOLD\' ) THEN CASE WHEN upper(PRODUCT) = \'LIVER CARE TABLETS\' THEN \'LIVER CARE TABS\' ELSE PRODUCT END ELSE \'OTHER\' END ORDER BY DATE DESC;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            