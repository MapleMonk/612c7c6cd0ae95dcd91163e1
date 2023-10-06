{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.Facebook_Catalog AS select p.onlinestoreurl, p.description, a.* FROM snitch_db.maplemonk.availability_master a LEFT JOIN Snitch_db.MAPLEMONK.SHOPIFYINDIA_PRODUCTS_GRAPH_QL p ON a.product_id = p.legacyresourceid",
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
                        