{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE rpsg_db.maplemonk.DRV_RETENTION_PRODUCT AS SELECT DATE_TRUNC(\'DAY\', \"DATE\") AS \"DATE\", CASE WHEN \"PRODUCT\" IN ( \'ACV EFFERVESCENT\', \'ACV JUICE\', \'ACV Pro\', \'DIABEX JUICE\', \'GUT CARE EFFERVESCENT\', \'HERBOSLIM\', \'LIVER CARE\', \'LIVER Care Tabs\', \'LIVER RESTORE\', \'PILES CARE\', \'Piles Spray \', \'Piles Spray Combo\', \'PILOCARE JUICE\', \'SHILAJIT GOLD\', \'SHILAJIT GOLD COMBO\', \'SOFTGEL\', \'SOFTGEL GOLD\' ) THEN \"PRODUCT\" ELSE \'OTHER\' -- For all products not in the specified list END AS \"PRODUCT\", SUM(IFNULL(spend, 0)) AS \"Spend\", SUM(IFNULL(REALISED_REVENUE, 0)) AS \"Delivered Revenue\" FROM rpsg_db.maplemonk.drv_product_roas WHERE upper(CHANNEL) = \'RETENTION\' AND ( \"PRODUCT\" IN ( \'ACV EFFERVESCENT\', \'ACV JUICE\', \'ACV Pro\', \'DIABEX JUICE\', \'GUT CARE EFFERVESCENT\', \'HERBOSLIM\', \'LIVER CARE\', \'LIVER Care Tabs\', \'LIVER RESTORE\', \'PILES CARE\', \'Piles Spray \', \'Piles Spray Combo\', \'PILOCARE JUICE\', \'SHILAJIT GOLD\', \'SHILAJIT GOLD COMBO\', \'SOFTGEL\', \'SOFTGEL GOLD\' ) OR \"PRODUCT\" NOT IN ( \'ACV EFFERVESCENT\', \'ACV JUICE\', \'ACV Pro\', \'DIABEX JUICE\', \'GUT CARE EFFERVESCENT\', \'HERBOSLIM\', \'LIVER CARE\', \'LIVER Care Tabs\', \'LIVER RESTORE\', \'PILES CARE\', \'Piles Spray \', \'Piles Spray Combo\', \'PILOCARE JUICE\', \'SHILAJIT GOLD\', \'SHILAJIT GOLD COMBO\', \'SOFTGEL\', \'SOFTGEL GOLD\' ) ) GROUP BY DATE_TRUNC(\'DAY\', \"DATE\"), CASE WHEN \"PRODUCT\" IN ( \'ACV EFFERVESCENT\', \'ACV JUICE\', \'ACV Pro\', \'DIABEX JUICE\', \'GUT CARE EFFERVESCENT\', \'HERBOSLIM\', \'LIVER CARE\', \'LIVER Care Tabs\', \'LIVER RESTORE\', \'PILES CARE\', \'Piles Spray \', \'Piles Spray Combo\', \'PILOCARE JUICE\', \'SHILAJIT GOLD\', \'SHILAJIT GOLD COMBO\', \'SOFTGEL\', \'SOFTGEL GOLD\' ) THEN \"PRODUCT\" ELSE \'OTHER\' END ORDER BY \"DATE\" DESC;",
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
            