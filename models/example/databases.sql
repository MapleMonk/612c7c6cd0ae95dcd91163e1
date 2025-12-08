{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE zeproc_db.maplemonk.ZEPROC_GSC_LATEST AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY PAGE ORDER BY DATE DESC) as rn FROM zeproc_db.maplemonk.ZEPROC_GSC_SEARCH_ANALYTICS_BY_PAGE ) WHERE rn = 1; CREATE OR REPLACE TABLE zeproc_db.maplemonk.GSC_and_catalog_product_master AS SELECT p.*, s.CTR, s.DATE, s.PAGE, s.CLICKS, s.POSITION, s.SITE_URL, s.IMPRESSIONS, s.SEARCH_TYPE, ROW_NUMBER() OVER (PARTITION BY p.ENTITY_ID ORDER BY s.CLICKS DESC NULLS LAST) as rn FROM zeproc_db.maplemonk.zeproc_catalog_product_master_final p LEFT JOIN zeproc_db.maplemonk.ZEPROC_GSC_LATEST s ON CONTAINS(s.PAGE, p.URL_KEY) QUALIFY rn = 1 ORDER BY p.ENTITY_ID;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ZEPROC_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            