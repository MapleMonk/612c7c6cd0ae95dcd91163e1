{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `izf-wh.maplemonk.izf_neon_market_fact_items` AS SELECT Product, Variant, CASE WHEN REGEXP_CONTAINS(LOWER(TRIM(SPLIT(Variant, \'/\')[SAFE_OFFSET(0)])), r\'^(xs|s|m|l|xl|xxl|xxxl|2xl|3xl|free size)$\') THEN TRIM(SPLIT(Variant, \'/\')[SAFE_OFFSET(0)]) ELSE TRIM(SPLIT(Variant, \'/\')[SAFE_OFFSET(1)]) END AS Size, COALESCE( INITCAP( REGEXP_EXTRACT( LOWER(Variant), r\'\b(black|white|red|blue|green|yellow|pink|beige|brown|grey|gray|maroon|navy|purple|cream|off[- ]white|mint)\b\' ) ), INITCAP( REGEXP_EXTRACT( LOWER(Product), r\'\b(black|white|red|blue|green|yellow|pink|beige|brown|grey|gray|maroon|navy|purple|cream|off[- ]white|mint)\b\' ) ) ) AS Color, CASE WHEN REGEXP_CONTAINS(LOWER(Product), r\'\bpants?\b\') THEN \'Pants\' WHEN REGEXP_CONTAINS(LOWER(Product), r\'\btop\b\') THEN \'Tops\' WHEN REGEXP_CONTAINS(LOWER(Product), r\'\bskirt\b\') THEN \'Skirts\' WHEN REGEXP_CONTAINS(LOWER(Product), r\'\bt-?shirt\b\') THEN \'T-Shirts\' WHEN REGEXP_CONTAINS(LOWER(Product), r\'\bjeans?\b\') THEN \'Jeans\' WHEN REGEXP_CONTAINS(LOWER(Product), r\'\bvest\b\') THEN \'Vest\' WHEN REGEXP_CONTAINS(LOWER(Product), r\'\bshrug\b\') THEN \'Shrug\' ELSE \'Other\' END AS product_category, PARSE_DATE(\'%Y-%m-%d\', Sold_On) AS sold_on, CAST(Qty AS INT64) AS quantity, CAST(Gross AS FLOAT64) AS gross, CAST(Neon_Market_Commission_25_ AS FLOAT64) AS neon_market_commission_25pct, CAST(Brand_Payout AS FLOAT64) AS brand_payout FROM `MapleMonk.IZF_Neon_Market_Sales` n LEFT JOIN ( SELECT * FROM ( SELECT SKU AS skucode, Product_Name AS name, category_name AS category, ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY 1) rw FROM `izf-wh.maplemonk.easyecom_izf_product_master` ) WHERE rw = 1 ) p ON LOWER(n.Product) = LOWER(p.name);",
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
            